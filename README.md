# sqs-demo

## コマンド

```bash
pnpm install
pnpm test
pnpm build
pnpm cdk synth
pnpm cdk deploy
pnpm cdk destroy
```

## 動作確認

```bash
STACK=SqsDemoStack
out() { aws cloudformation describe-stacks --stack-name $STACK \
  --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text; }
QUEUE_URL=$(out QueueUrl)
DLQ_URL=$(out DeadLetterQueueUrl)
SM_ARN=$(out StateMachineArn)

# メッセージ送信
aws sqs send-message --queue-url "$QUEUE_URL" --message-body '{"itemId":"item-001"}'
aws sqs send-message --queue-url "$QUEUE_URL" --message-body '{"itemId":"item-002","waitSeconds":300}'

# SFN の実行一覧
aws stepfunctions list-executions --state-machine-arn "$SM_ARN" \
  --query 'executions[].{name:name,status:status}' --output table

# DLQ の確認
aws sqs receive-message --queue-url "$DLQ_URL" --attribute-names ApproximateReceiveCount

# Lambda のログ
aws logs tail "/aws/lambda/$(aws cloudformation describe-stack-resources --stack-name $STACK \
  --query "StackResources[?ResourceType=='AWS::Lambda::Function'].PhysicalResourceId" --output text)" --follow
```
