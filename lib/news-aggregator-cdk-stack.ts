import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as eventsources from 'aws-cdk-lib/aws-lambda-event-sources';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';

export class NewsAggregatorCdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const feedTable = new dynamodb.Table(this, 'FeedTable', {
      partitionKey: { name: 'PK', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'SK', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      stream: dynamodb.StreamViewType.NEW_IMAGE,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });

    // Create the dead letter queue
    const feedDLQueue = new sqs.Queue(this, 'FeedDLQueue', {
      queueName: 'FeedDLQueue',
      retentionPeriod: cdk.Duration.days(1)
    });

    // Create the main SQS queue with the redrive policy
    const feedQueue = new sqs.Queue(this, 'FeedQueue', {
      queueName: 'FeedQueue',
      visibilityTimeout: cdk.Duration.seconds(30),
      retentionPeriod: cdk.Duration.days(1),
      deadLetterQueue: {
        queue: feedDLQueue,
        maxReceiveCount: 5
      }
    });

    // Create the FeedManager Lambda
    const feedManagerLambda = new lambda.Function(this, 'FeedManagerLambda', {
      description: 'Lambda function to manage feeds',
      runtime: lambda.Runtime.PYTHON_3_10,
      handler: 'index.handler',
      code: lambda.Code.fromAsset(
        path.join(__dirname, '../lambdas/python/feed_manager'),
        {
          bundling: {
            image: lambda.Runtime.PYTHON_3_10.bundlingImage,
            command: [
              'bash',
              '-c',
              'pip install -r requirements.txt -t /asset-output && cp -au . /asset-output'
            ]
          }
        }
      ),
      architecture: lambda.Architecture.ARM_64,
      timeout: cdk.Duration.seconds(10),
      environment: {
        TABLE_NAME: feedTable.tableName
      }
    });

    feedManagerLambda.addEnvironment(
      'POWERTOOLS_METRICS_NAMESPACE',
      'ChainProfile/NewsFeedAggregator/FeedManager'
    );
    feedManagerLambda.addEnvironment('POWERTOOLS_SERVICE_NAME', 'FeedManager');

    const feedItemInitLambda = new lambda.Function(this, 'FeedItemInitLambda', {
      description: 'Lambda function to initialize feed items',
      runtime: lambda.Runtime.PYTHON_3_10,
      handler: 'index.stream_handler',
      code: lambda.Code.fromAsset(
        path.join(__dirname, '../lambdas/python/feed_item_manager'),
        {
          bundling: {
            image: lambda.Runtime.PYTHON_3_10.bundlingImage,
            command: [
              'bash',
              '-c',
              'pip install -r requirements.txt -t /asset-output && cp -au . /asset-output'
            ]
          }
        }
      ),
      architecture: lambda.Architecture.ARM_64,
      timeout: cdk.Duration.seconds(10),
      environment: {
        TABLE_NAME: feedTable.tableName,
        QUEUE_URL: feedQueue.queueUrl
      }
    });

    // Create the FeedItemFetcher Lambda
    const feedItemFetcherLambda = new lambda.Function(
      this,
      'FeedItemFetcherLambda',
      {
        description: 'Lambda function to fetch feed items',
        runtime: lambda.Runtime.PYTHON_3_10,
        handler: 'index.feed_message_handler',
        code: lambda.Code.fromAsset(
          path.join(__dirname, '../lambdas/python/feed_item_manager'),
          {
            bundling: {
              image: lambda.Runtime.PYTHON_3_10.bundlingImage,
              command: [
                'bash',
                '-c',
                'pip install -r requirements.txt -t /asset-output && cp -au . /asset-output'
              ]
            }
          }
        ),
        architecture: lambda.Architecture.ARM_64,
        timeout: cdk.Duration.seconds(10),
        environment: {
          TABLE_NAME: feedTable.tableName,
          QUEUE_URL: feedQueue.queueUrl
        }
      }
    );

    // Create the FeedScheduler Lambda
    const feedSchedulerLambda = new lambda.Function(
      this,
      'FeedSchedulerLambda',
      {
        description: 'Lambda function to schedule feeds',
        runtime: lambda.Runtime.PYTHON_3_10,
        handler: 'index.handler',
        code: lambda.Code.fromAsset(
          path.join(__dirname, '../lambdas/python/feed_scheduler'),
          {
            bundling: {
              image: lambda.Runtime.PYTHON_3_10.bundlingImage,
              command: [
                'bash',
                '-c',
                'pip install -r requirements.txt -t /asset-output && cp -au . /asset-output'
              ]
            }
          }
        ),
        architecture: lambda.Architecture.ARM_64,
        timeout: cdk.Duration.seconds(10),
        environment: {
          TABLE_NAME: feedTable.tableName,
          QUEUE_URL: feedQueue.queueUrl
        }
      }
    );

    feedTable.grantReadWriteData(feedManagerLambda);
    feedTable.grantReadWriteData(feedItemInitLambda);
    feedTable.grantReadWriteData(feedItemFetcherLambda);
    feedTable.grantReadWriteData(feedSchedulerLambda);

    feedQueue.grantSendMessages(feedSchedulerLambda);

    feedItemInitLambda.addEventSource(
      new eventsources.DynamoEventSource(feedTable, {
        startingPosition: lambda.StartingPosition.TRIM_HORIZON,
        batchSize: 10,
        bisectBatchOnError: true,
        retryAttempts: 10
      })
    );

    feedTable.grantStreamRead(feedItemInitLambda);

    feedItemFetcherLambda.addEventSource(
      new eventsources.SqsEventSource(feedQueue, {
        batchSize: 10
      })
    );

    feedQueue.grantConsumeMessages(feedItemFetcherLambda);

    // Create a CloudWatch Event Rule to trigger on a schedule
    const feedSchedulerRule = new events.Rule(this, 'FeedSchedulerRule', {
      schedule: events.Schedule.rate(cdk.Duration.minutes(5)),
      description: 'Rule to trigger the FeedScheduler Lambda every 15 minutes',
      targets: [new targets.LambdaFunction(feedSchedulerLambda)]
    });

    const feedManagerApi = new apigateway.RestApi(this, 'FeedManagerApi', {
      restApiName: 'Feed Manager API',
      description: 'API to manage feeds'
    });

    const feedManagerLambdaIntegration = new apigateway.LambdaIntegration(
      feedManagerLambda
    );

    // Create api gateway resource
    const feedManagerResource = feedManagerApi.root.addResource('feeds');

    // POST /feeds - Adds a new feed
    feedManagerResource.addMethod('POST', feedManagerLambdaIntegration);

    new cdk.CfnOutput(this, 'FeedManagerApiUrl', {
      value: feedManagerApi.url
    });
  }
}
