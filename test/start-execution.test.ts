import {
  DescribeExecutionCommand,
  ExecutionAlreadyExists,
  ExecutionDoesNotExist,
  SFNClient,
  StartExecutionCommand,
} from '@aws-sdk/client-sfn';
import { DuplicateExecutionError, GenerationExhaustedError, InvalidMessageError } from '../lambda/starter/errors';
import {
  ExecutionInput,
  parseMessageBody,
  startExecutionWithoutDuplication,
  toExecutionArn,
} from '../lambda/starter/start-execution';

const SM_ARN = 'arn:aws:states:ap-northeast-1:123456789012:stateMachine:DemoSM';

/**
 * Step Functions の実行履歴をメモリ上で再現するフェイククライアント。
 * existing に「実行名 → { status, input }」を入れておくと、その名前は使用済みとして振る舞う。
 */
function fakeSfn(existing: Record<string, { status: string; input?: string }> = {}) {
  const calls: string[] = [];
  const client = {
    send: jest.fn(async (cmd: unknown) => {
      if (cmd instanceof StartExecutionCommand) {
        const name = cmd.input.name!;
        calls.push(`start:${name}`);
        if (existing[name]) throw new ExecutionAlreadyExists({ message: 'exists', $metadata: {} });
        existing[name] = { status: 'RUNNING', input: cmd.input.input };
        return { executionArn: toExecutionArn(SM_ARN, name) };
      }
      if (cmd instanceof DescribeExecutionCommand) {
        const name = cmd.input.executionArn!.split(':').pop()!;
        calls.push(`describe:${name}`);
        const e = existing[name];
        if (!e) throw new ExecutionDoesNotExist({ message: 'not found', $metadata: {} });
        return { status: e.status, input: e.input };
      }
      throw new Error('unexpected command');
    }),
  };
  return { client: client as unknown as Pick<SFNClient, 'send'>, calls, existing };
}

const input = (overrides: Partial<ExecutionInput> = {}): ExecutionInput => ({
  itemId: 'item-001',
  waitSeconds: 30,
  sqsMessageId: 'msg-new',
  sentAt: '2026-09-30T00:00:00.000Z',
  ...overrides,
});

const deps = (client: Pick<SFNClient, 'send'>, maxGeneration = 1000) => ({ sfn: client, stateMachineArn: SM_ARN, maxGeneration });

describe('startExecutionWithoutDuplication', () => {
  test('初回は世代 1 で起動する', async () => {
    const { client } = fakeSfn();
    const res = await startExecutionWithoutDuplication(input(), deps(client));
    expect(res).toMatchObject({ executionName: 'item-001__1', generation: 1, alreadyStarted: false });
  });

  test('過去の実行が完了済みなら、次の空き世代で起動する', async () => {
    const { client, calls } = fakeSfn({
      'item-001__1': { status: 'SUCCEEDED', input: '{"sqsMessageId":"old1"}' },
      'item-001__2': { status: 'FAILED', input: '{"sqsMessageId":"old2"}' },
    });
    const res = await startExecutionWithoutDuplication(input(), deps(client));
    expect(res.executionName).toBe('item-001__3');
    expect(calls).toEqual([
      'start:item-001__1', 'describe:item-001__1',
      'start:item-001__2', 'describe:item-001__2',
      'start:item-001__3',
    ]);
  });

  test('同じ itemId が RUNNING なら DuplicateExecutionError(→ SQS でリトライ)', async () => {
    const { client } = fakeSfn({
      'item-001__1': { status: 'SUCCEEDED', input: '{"sqsMessageId":"old1"}' },
      'item-001__2': { status: 'RUNNING', input: '{"sqsMessageId":"other"}' },
    });
    await expect(startExecutionWithoutDuplication(input(), deps(client))).rejects.toBeInstanceOf(DuplicateExecutionError);
  });

  test('別の itemId の実行には影響されない', async () => {
    const { client } = fakeSfn({ 'item-002__1': { status: 'RUNNING', input: '{}' } });
    const res = await startExecutionWithoutDuplication(input(), deps(client));
    expect(res.executionName).toBe('item-001__1');
  });

  test('同じ SQS メッセージから起動済みの実行があれば、成功扱い(二重起動しない)', async () => {
    const { client } = fakeSfn({
      'item-001__1': { status: 'SUCCEEDED', input: '{"sqsMessageId":"msg-new"}' },
    });
    const res = await startExecutionWithoutDuplication(input(), deps(client));
    expect(res).toMatchObject({ executionName: 'item-001__1', alreadyStarted: true });
  });

  test('DescribeExecution が結果整合性で見つからない場合は、実行中とみなす', async () => {
    const { client } = fakeSfn();
    // Start は「存在する」と言うが Describe では見えない状況を再現
    (client.send as jest.Mock).mockImplementationOnce(async () => {
      throw new ExecutionAlreadyExists({ message: 'exists', $metadata: {} });
    });
    await expect(startExecutionWithoutDuplication(input(), deps(client))).rejects.toBeInstanceOf(DuplicateExecutionError);
  });

  test('2 つの Lambda が同時に処理しても、起動できるのは片方だけ', async () => {
    const { client } = fakeSfn({ 'item-001__1': { status: 'SUCCEEDED', input: '{"sqsMessageId":"old"}' } });
    const results = await Promise.allSettled([
      startExecutionWithoutDuplication(input({ sqsMessageId: 'msg-A' }), deps(client)),
      startExecutionWithoutDuplication(input({ sqsMessageId: 'msg-B' }), deps(client)),
    ]);
    expect(results.filter((r) => r.status === 'fulfilled')).toHaveLength(1);
    const rejected = results.find((r) => r.status === 'rejected') as PromiseRejectedResult;
    expect(rejected.reason).toBeInstanceOf(DuplicateExecutionError);
  });

  test('世代番号が上限に達したら GenerationExhaustedError', async () => {
    const { client } = fakeSfn({
      'item-001__1': { status: 'SUCCEEDED', input: '{}' },
      'item-001__2': { status: 'SUCCEEDED', input: '{}' },
    });
    await expect(startExecutionWithoutDuplication(input(), deps(client, 2))).rejects.toBeInstanceOf(GenerationExhaustedError);
  });
});

describe('parseMessageBody', () => {
  test('waitSeconds 省略時は 30 秒', () => {
    expect(parseMessageBody('{"itemId":"item-001"}')).toEqual({ itemId: 'item-001', waitSeconds: 30 });
  });

  test.each([
    ['JSON でない', 'not json'],
    ['itemId が無い', '{}'],
    ['itemId に使えない文字', '{"itemId":"item:001"}'],
    ['itemId が長すぎる', JSON.stringify({ itemId: 'a'.repeat(71) })],
    ['waitSeconds が範囲外', '{"itemId":"item-001","waitSeconds":301}'],
  ])('%s → InvalidMessageError', (_, body) => {
    expect(() => parseMessageBody(body)).toThrow(InvalidMessageError);
  });
});
