/**
 * Lambda が throw するエラークラス群。
 *
 * どのエラーも最終的には「Lambda の失敗」として SQS に戻り、
 * visibility timeout 経過後に再配信 → maxReceiveCount 超過で DLQ へ移動する。
 * クラス名をログに出すことで、DLQ 調査時に「なぜ失敗したか」を判別しやすくしている。
 */

/** メッセージ本文が不正(JSON でない / itemId が無い・不正な文字を含む など) */
export class InvalidMessageError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "InvalidMessageError";
	}
}

/**
 * 同じ itemId の SFN 実行がまだ RUNNING のため起動を見送った。
 * SQS のリトライで「実行が終わるのを待つ」ために、あえて throw する。
 */
export class DuplicateExecutionError extends Error {
	constructor(
		public readonly itemId: string,
		public readonly runningExecutionName: string,
	) {
		super(
			`Execution for itemId="${itemId}" is still running: ${runningExecutionName}`,
		);
		this.name = "DuplicateExecutionError";
	}
}

/** 世代番号が上限(MAX_GENERATION)に達し、空き番号を見つけられなかった */
export class GenerationExhaustedError extends Error {
	constructor(
		public readonly itemId: string,
		public readonly maxGeneration: number,
	) {
		super(
			`No free generation for itemId="${itemId}" (checked up to ${maxGeneration})`,
		);
		this.name = "GenerationExhaustedError";
	}
}
