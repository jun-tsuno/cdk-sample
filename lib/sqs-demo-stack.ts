import * as path from "node:path";
import * as cdk from "aws-cdk-lib/core";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as lambdaNodejs from "aws-cdk-lib/aws-lambda-nodejs";
import * as logs from "aws-cdk-lib/aws-logs";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as sqs from "aws-cdk-lib/aws-sqs";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { Construct } from "constructs";

/**
 * リトライ挙動を調整するためのパラメータ。
 *
 * 重複時(同じ itemId が実行中)に待てるおおよその時間は
 *   visibilityTimeout × (maxReceiveCount - 1)
 * となる。デフォルト値では 60 秒 × 4 = 約 4 分。
 */
export interface SqsDemoStackProps extends cdk.StackProps {
	/** Lambda のタイムアウト。@default 10 秒 */
	readonly lambdaTimeout?: cdk.Duration;
	/** メインキューの visibility timeout = リトライ間隔。@default 60 秒 */
	readonly visibilityTimeout?: cdk.Duration;
	/** この回数だけ受信(=失敗)したら DLQ に移す。@default 5 */
	readonly maxReceiveCount?: number;
	/** SFN 実行のタイムアウト。@default 10 分 */
	readonly stateMachineTimeout?: cdk.Duration;
	/** 世代番号の探索上限。@default 1000 */
	readonly maxGeneration?: number;
}

/**
 * SQS → Lambda → Step Functions のサンプルスタック。
 *
 *   SQS(DLQ あり) ─batchSize 1─▶ Lambda ─StartExecution─▶ SFN(Step1_Wait → Step2_Complete)
 *   メッセージ: { "itemId": "item-001", "waitSeconds": 30 }
 *
 * ■ 重複起動の防止(世代番号方式)
 *   実行名を `${itemId}__${gen}` とし、gen = 1, 2, ... の順に起動を試す。
 *   同名の実行が RUNNING なら throw してリトライ、完了済みなら次の gen へ進む。
 *   Standard Workflow は同じ実行名を 1 つしか作れないため、同時処理でも起動は 1 つだけになる。
 *
 * ■ リトライと DLQ
 *   重複時は visibility timeout(60 秒)ごとに再配信され、5 回失敗すると DLQ へ移る(約 4 分待つ)。
 *
 * ■ 制約
 *   API 呼び出し回数は同じ itemId の過去 90 日の実行件数に比例する。
 */
export class SqsDemoStack extends cdk.Stack {
	constructor(scope: Construct, id: string, props: SqsDemoStackProps = {}) {
		super(scope, id, props);

		const lambdaTimeout = props.lambdaTimeout ?? cdk.Duration.seconds(10);
		const visibilityTimeout =
			props.visibilityTimeout ?? cdk.Duration.seconds(60);
		const maxReceiveCount = props.maxReceiveCount ?? 5;
		const stateMachineTimeout =
			props.stateMachineTimeout ?? cdk.Duration.minutes(10);
		const maxGeneration = props.maxGeneration ?? 1000;

		// =========================================================================
		// SQS: メインキュー + DLQ
		// =========================================================================

		// リトライし尽くしたメッセージの退避先。調査できるよう保持期間は最大の 14 日
		const deadLetterQueue = new sqs.Queue(this, "DeadLetterQueue", {
			retentionPeriod: cdk.Duration.days(14),
			enforceSSL: true,
		});

		const queue = new sqs.Queue(this, "MainQueue", {
			// Lambda が失敗したメッセージは、この時間が経つまで再配信されない。
			// つまり visibility timeout が「リトライ間隔」になる。
			// (AWS 推奨: Lambda タイムアウトの 6 倍以上)
			visibilityTimeout,
			deadLetterQueue: {
				queue: deadLetterQueue,
				maxReceiveCount, // 受信回数がこれを超えたら DLQ へ
			},
			enforceSSL: true,
		});

		// =========================================================================
		// Step Functions: 2 ステップの簡単なワークフロー
		//   Step1_Wait     : input.waitSeconds 秒待つ(重い処理の代わり)
		//   Step2_Complete : 結果を整形して終了
		// =========================================================================

		const step1Wait = new sfn.Wait(this, "Step1_Wait", {
			time: sfn.WaitTime.secondsPath("$.waitSeconds"),
		});

		const step2Complete = new sfn.Pass(this, "Step2_Complete", {
			parameters: {
				"itemId.$": "$.itemId",
				"sqsMessageId.$": "$.sqsMessageId",
				result: "done",
				"finishedAt.$": "$$.State.EnteredTime", // コンテキストオブジェクトから現在時刻を取得
			},
		});

		const stateMachine = new sfn.StateMachine(this, "StateMachine", {
			// Standard: 実行名が一意であることが保証される(重複防止の前提)。Express では不可
			stateMachineType: sfn.StateMachineType.STANDARD,
			definitionBody: sfn.DefinitionBody.fromChainable(
				step1Wait.next(step2Complete),
			),
			timeout: stateMachineTimeout,
		});

		// =========================================================================
		// Lambda: SQS メッセージを受け取り、重複チェックしながら SFN を起動する
		// =========================================================================

		const starterFunction = new lambdaNodejs.NodejsFunction(
			this,
			"StarterFunction",
			{
				entry: path.join(__dirname, "../lambda/starter/index.ts"),
				handler: "handler",
				runtime: lambda.Runtime.NODEJS_22_X,
				architecture: lambda.Architecture.ARM_64,
				timeout: lambdaTimeout,
				environment: {
					STATE_MACHINE_ARN: stateMachine.stateMachineArn,
					MAX_GENERATION: String(maxGeneration),
				},
				logGroup: new logs.LogGroup(this, "StarterFunctionLogGroup", {
					retention: logs.RetentionDays.ONE_WEEK,
					removalPolicy: cdk.RemovalPolicy.DESTROY,
				}),
				// @aws-sdk/* は Lambda ランタイムに同梱されているため、デフォルトでバンドル対象外
			},
		);

		// 権限 1: SFN の起動
		stateMachine.grantStartExecution(starterFunction);

		// 権限 2: 既存実行の状態確認(DescribeExecution)。対象はこのステートマシンの実行だけに絞る
		starterFunction.addToRolePolicy(
			new iam.PolicyStatement({
				actions: ["states:DescribeExecution"],
				resources: [
					this.formatArn({
						service: "states",
						resource: "execution",
						resourceName: `${stateMachine.stateMachineName}:*`,
						arnFormat: cdk.ArnFormat.COLON_RESOURCE_NAME,
					}),
				],
			}),
		);

		// SQS → Lambda のイベントソースマッピング。1 回の呼び出しで 1 メッセージだけ処理する
		starterFunction.addEventSource(new SqsEventSource(queue, { batchSize: 1 }));

		// =========================================================================
		// 出力(動作確認で使う値)
		// =========================================================================
		new cdk.CfnOutput(this, "QueueUrl", { value: queue.queueUrl });
		new cdk.CfnOutput(this, "DeadLetterQueueUrl", {
			value: deadLetterQueue.queueUrl,
		});
		new cdk.CfnOutput(this, "StateMachineArn", {
			value: stateMachine.stateMachineArn,
		});
	}
}
