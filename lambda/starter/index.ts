/**
 * SQS から起動され、Step Functions を起動する Lambda のエントリーポイント。
 *
 * - batchSize = 1 のため、1 回の呼び出しで処理するメッセージは 1 件。
 * - 処理に失敗した(throw した)メッセージは SQS に残り、visibility timeout 経過後に再配信される。
 *   受信回数が maxReceiveCount を超えると DLQ に移動する。
 * - 同じ itemId の SFN が実行中の場合も「失敗」として throw し、上記のリトライに乗せる。
 */
import type { SQSEvent, SQSRecord } from "aws-lambda";
import { SFNClient } from "@aws-sdk/client-sfn";
import {
	parseMessageBody,
	startExecutionWithoutDuplication,
} from "./start-execution";

// 環境変数は CDK スタック(lib/sqs-demo-stack.ts)で設定される
const STATE_MACHINE_ARN = process.env.STATE_MACHINE_ARN!;
const MAX_GENERATION = Number(process.env.MAX_GENERATION ?? "1000");

// クライアントはハンドラーの外で生成し、ウォームスタート時に再利用する
const sfn = new SFNClient({});

/** CloudWatch Logs で検索しやすいよう JSON 1 行で出力する */
const log = (entry: Record<string, unknown>) =>
	console.log(JSON.stringify(entry));

export const handler = async (event: SQSEvent): Promise<void> => {
	// batchSize = 1 なので実質 1 件だけ。
	// 複数件になった場合でも、1 件でも throw するとバッチ全体が失敗扱いになる点に注意。
	for (const record of event.Records) {
		await processRecord(record);
	}
};

async function processRecord(record: SQSRecord): Promise<void> {
	const context = {
		messageId: record.messageId,
		receiveCount: Number(record.attributes.ApproximateReceiveCount), // 何回目の受信か(リトライ状況の確認用)
	};

	try {
		const body = parseMessageBody(record.body);
		const result = await startExecutionWithoutDuplication(
			{
				itemId: body.itemId,
				waitSeconds: body.waitSeconds,
				sqsMessageId: record.messageId,
				// 再配信でも値が変わらないよう、処理時刻ではなく SQS の送信時刻を使う
				sentAt: new Date(Number(record.attributes.SentTimestamp)).toISOString(),
			},
			{
				sfn,
				stateMachineArn: STATE_MACHINE_ARN,
				maxGeneration: MAX_GENERATION,
				log: (e) => log({ ...context, ...e }),
			},
		);
		log({ ...context, msg: "Done", ...result });
	} catch (err) {
		// エラー種別(DuplicateExecutionError など)をログに残してから再 throw → SQS がリトライ
		const e = err as Error;
		log({
			...context,
			msg: "Failed",
			errorName: e.name,
			errorMessage: e.message,
		});
		throw err;
	}
}
