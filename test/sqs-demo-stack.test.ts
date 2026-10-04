import * as cdk from 'aws-cdk-lib/core';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { SqsDemoStack } from '../lib/sqs-demo-stack';

describe('SqsDemoStack', () => {
  const app = new cdk.App();
  const template = Template.fromStack(new SqsDemoStack(app, 'TestStack'));

  test('メインキューに DLQ(maxReceiveCount=5)と visibility timeout 60 秒が設定される', () => {
    template.hasResourceProperties('AWS::SQS::Queue', {
      VisibilityTimeout: 60,
      RedrivePolicy: { maxReceiveCount: 5, deadLetterTargetArn: Match.anyValue() },
    });
  });

  test('SQS → Lambda のバッチサイズは 1', () => {
    template.hasResourceProperties('AWS::Lambda::EventSourceMapping', { BatchSize: 1 });
  });

  test('ステートマシンは STANDARD タイプ', () => {
    template.hasResourceProperties('AWS::StepFunctions::StateMachine', { StateMachineType: 'STANDARD' });
  });

  test('Lambda に StartExecution と DescribeExecution の権限がある', () => {
    template.hasResourceProperties('AWS::IAM::Policy', {
      PolicyDocument: {
        Statement: Match.arrayWith([
          Match.objectLike({ Action: 'states:StartExecution' }),
          Match.objectLike({ Action: 'states:DescribeExecution' }),
        ]),
      },
    });
  });
});
