/**
 * 「同じ itemId の SFN を多重起動しない」ための起動ロジック本体。
 *
 * ■ 仕組み(世代番号方式)
 *   実行名を `${itemId}__${gen}` とし、gen = 1, 2, 3 ... の順に StartExecution を試す。
 *   Step Functions (Standard) は「同じ実行名は 1 つしか存在できない」ことを
 *   アトミックに保証するので、これを排他ロックの代わりに使う。
 *
 *     StartExecution 成功             → その gen が今回の世代。起動完了
 *     ExecutionAlreadyExists          → その名前の実行の状態を確認
 *        ├ RUNNING                    → 実行中なので DuplicateExecutionError(SQS でリトライ)
 *        └ SUCCEEDED/FAILED/...       → 使用済みの番号なので次の gen へ
 *
 *   同時に 2 つの Lambda が同じ itemId を処理しても、両者は同じ履歴を見て
 *   同じ gen にたどり着くため、起動に成功するのは必ず片方だけになる。
 *
 * テストしやすいよう、SFN クライアントや設定値は引数で受け取る(index.ts で注入)。
 */
import {
	DescribeExecutionCommand,
	ExecutionAlreadyExists,
	ExecutionDoesNotExist,
	SFNClient,
	StartExecutionCommand,
} from "@aws-sdk/client-sfn";
import {
	DuplicateExecutionError,
	GenerationExhaustedError,
	InvalidMessageError,
} from "./errors";

/** 実行名の区切り文字。`${itemId}${SEPARATOR}${gen}` の形になる */
export const SEPARATOR = "__";

/**
 * itemId に使える文字と長さ。
 * SFN の実行名は 80 文字以内かつ使用できる文字が限られるため、
 * 区切り文字 + 世代番号の分(最大 10 文字)を差し引いて 70 文字までとする。
 */
const ITEM_ID_PATTERN = /^[A-Za-z0-9_-]{1,70}$/;

/** SFN の Wait ステップで待つ秒数のデフォルト値と上限 */
const DEFAULT_WAIT_SECONDS = 30;
const MAX_WAIT_SECONDS = 300;

/** SQS メッセージ本文として受け付ける形式 */
export interface MessageBody {
	itemId: string;
	/** 省略可。SFN の Step1 (Wait) で待つ秒数。動作確認用 */
	waitSeconds?: number;
}

/** SFN に渡す input */
export interface ExecutionInput {
	itemId: string;
	waitSeconds: number;
	/** どの SQS メッセージから起動されたか(再配信時の判定に使う) */
	sqsMessageId: string;
	/** SQS にメッセージが送信された時刻(ISO 8601) */
	sentAt: string;
}

export interface StartDeps {
	sfn: Pick<SFNClient, "send">;
	stateMachineArn: string;
	maxGeneration: number;
	log?: (entry: Record<string, unknown>) => void;
}

export interface StartResult {
	executionName: string;
	executionArn: string;
	generation: number;
	/** true: 同じ SQS メッセージの再配信で、すでに起動済みだった */
	alreadyStarted: boolean;
}

/**
 * SQS メッセージ本文をパース・検証する。
 * 不正な場合は InvalidMessageError を throw(→ リトライを経て DLQ へ)。
 */
export function parseMessageBody(
	body: string,
): MessageBody & { waitSeconds: number } {
	let parsed: unknown;
	try {
		parsed = JSON.parse(body);
	} catch {
		throw new InvalidMessageError("Message body is not valid JSON");
	}
	if (typeof parsed !== "object" || parsed === null) {
		throw new InvalidMessageError("Message body must be a JSON object");
	}

	const { itemId, waitSeconds } = parsed as Record<string, unknown>;
	if (typeof itemId !== "string" || !ITEM_ID_PATTERN.test(itemId)) {
		throw new InvalidMessageError(
			`itemId must match ${ITEM_ID_PATTERN}, got: ${JSON.stringify(itemId)}`,
		);
	}
	if (
		waitSeconds !== undefined &&
		(typeof waitSeconds !== "number" ||
			!Number.isInteger(waitSeconds) ||
			waitSeconds < 0 ||
			waitSeconds > MAX_WAIT_SECONDS)
	) {
		throw new InvalidMessageError(
			`waitSeconds must be an integer between 0 and ${MAX_WAIT_SECONDS}`,
		);
	}

	return {
		itemId,
		waitSeconds: (waitSeconds as number | undefined) ?? DEFAULT_WAIT_SECONDS,
	};
}

/**
 * ステートマシン ARN と実行名から実行 ARN を組み立てる。
 *   arn:aws:states:{region}:{account}:stateMachine:{SM名}
 *   → arn:aws:states:{region}:{account}:execution:{SM名}:{実行名}
 * (DescribeExecution のために、一覧取得をせずに ARN を特定できる)
 */
export function toExecutionArn(
	stateMachineArn: string,
	executionName: string,
): string {
	return `${stateMachineArn.replace(":stateMachine:", ":execution:")}:${executionName}`;
}

/**
 * 世代番号を 1 から探索し、空いている番号で SFN を起動する。
 * SFN の完了は待たない(StartExecution は起動したら即座に返る非同期 API)。
 */
export async function startExecutionWithoutDuplication(
	input: ExecutionInput,
	deps: StartDeps,
): Promise<StartResult> {
	const { sfn, stateMachineArn, maxGeneration, log = () => {} } = deps;
	const inputJson = JSON.stringify(input);

	for (let gen = 1; gen <= maxGeneration; gen++) {
		const executionName = `${input.itemId}${SEPARATOR}${gen}`;

		// --- 1. まず起動を試みる。「空き確認」と「起動」を 1 回の API で行うのがポイント ---
		try {
			const res = await sfn.send(
				new StartExecutionCommand({
					stateMachineArn,
					name: executionName,
					input: inputJson,
				}),
			);
			// 補足: 同じ名前・同じ input の実行が RUNNING 中だと、SFN はエラーにせず
			//       既存実行の ARN を返す(冪等動作)。input に sqsMessageId を含めているため、
			//       これが起きるのは「同じメッセージの再配信」のときだけで、起動済み扱いで問題ない。
			log({ msg: "StartExecution succeeded", executionName, generation: gen });
			return {
				executionName,
				executionArn: res.executionArn!,
				generation: gen,
				alreadyStarted: false,
			};
		} catch (err) {
			if (!(err instanceof ExecutionAlreadyExists)) throw err; // スロットリング等はそのまま失敗させる
		}

		// --- 2. 同名の実行が存在した → その実行の状態を確認する ---
		const executionArn = toExecutionArn(stateMachineArn, executionName);
		let status: string | undefined;
		let existingInput: string | undefined;
		try {
			const desc = await sfn.send(
				new DescribeExecutionCommand({ executionArn }),
			);
			status = desc.status;
			existingInput = desc.input;
		} catch (err) {
			// DescribeExecution は結果整合性のため、起動直後の実行が「存在しない」と返ることがある。
			// 安全側に倒して「実行中」とみなし、SQS のリトライに任せる。
			if (err instanceof ExecutionDoesNotExist) {
				log({
					msg: "Execution not visible yet, treat as running",
					executionName,
				});
				throw new DuplicateExecutionError(input.itemId, executionName);
			}
			throw err;
		}

		// 同じ SQS メッセージから既に起動済み(Lambda が起動後に失敗して再配信された等)なら、
		// 二重に処理しないよう成功扱いで終える。
		if (
			existingInput !== undefined &&
			safeParse(existingInput)?.sqsMessageId === input.sqsMessageId
		) {
			log({ msg: "Already started by this message", executionName, status });
			return {
				executionName,
				executionArn,
				generation: gen,
				alreadyStarted: true,
			};
		}

		if (status === "RUNNING") {
			// 同じ itemId が実行中 → 起動しない。throw して SQS に再試行させる
			log({ msg: "Duplicate: execution is running", executionName });
			throw new DuplicateExecutionError(input.itemId, executionName);
		}

		// SUCCEEDED / FAILED / TIMED_OUT / ABORTED 等 → この番号は使用済み。次の世代を試す
		log({ msg: "Generation already used, try next", executionName, status });
	}

	throw new GenerationExhaustedError(input.itemId, maxGeneration);
}

function safeParse(json: string): Record<string, unknown> | undefined {
	try {
		return JSON.parse(json) as Record<string, unknown>;
	} catch {
		return undefined;
	}
}
