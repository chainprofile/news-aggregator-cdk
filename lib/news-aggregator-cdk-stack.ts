import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as eventsources from 'aws-cdk-lib/aws-lambda-event-sources';

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

    const feedItemCreatorLambda = new lambda.Function(
      this,
      'FeedItemCreatorLambda',
      {
        description: 'Lambda function to create feed items',
        runtime: lambda.Runtime.PYTHON_3_10,
        handler: 'index.stream_handler',
        code: lambda.Code.fromAsset(
          path.join(__dirname, '../lambdas/python/feed_item_creator'),
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
      }
    );

    const webSubSubscriptionLambda = new lambda.Function(
      this,
      'WebSubSubscriptionLambda',
      {
        description: 'Lambda function to subscribe to web sub feeds',
        runtime: lambda.Runtime.PYTHON_3_10,
        handler: 'index.handler',
        code: lambda.Code.fromAsset(
          path.join(__dirname, '../lambdas/python/web_sub_subscription'),
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
      }
    );

    feedTable.grantReadWriteData(feedManagerLambda);
    feedTable.grantReadWriteData(feedItemCreatorLambda);
    feedTable.grantReadWriteData(webSubSubscriptionLambda);

    feedItemCreatorLambda.addEventSource(
      new eventsources.DynamoEventSource(feedTable, {
        startingPosition: lambda.StartingPosition.TRIM_HORIZON,
        batchSize: 10,
        bisectBatchOnError: true,
        retryAttempts: 10
      })
    );

    feedTable.grantStreamRead(feedItemCreatorLambda);

    webSubSubscriptionLambda.addEventSource(
      new eventsources.DynamoEventSource(feedTable, {
        startingPosition: lambda.StartingPosition.TRIM_HORIZON,
        batchSize: 10,
        bisectBatchOnError: true,
        retryAttempts: 10
      })
    );

    feedTable.grantStreamRead(webSubSubscriptionLambda);

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
