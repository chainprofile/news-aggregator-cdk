import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';

export class NewsAggregatorCdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const feedTable = new dynamodb.Table(this, 'FeedTable', {
      partitionKey: { name: 'PK', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'SK', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });

    const feedManagerLambda = new cdk.aws_lambda.Function(
      this,
      'FeedManagerLambda',
      {
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
      }
    );

    feedManagerLambda.addEnvironment(
      'POWERTOOLS_METRICS_NAMESPACE',
      'ChainProfile/NewsFeedAggregator/FeedManager'
    );
    feedManagerLambda.addEnvironment('POWERTOOLS_SERVICE_NAME', 'FeedManager');

    feedTable.grantReadWriteData(feedManagerLambda);

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

    // GET /feeds - Retrieves a list of active feeds
    feedManagerResource.addMethod('GET', feedManagerLambdaIntegration);

    // Create a resource for specific feeds: /feeds/{feed_id}
    const singleFeedResource = feedManagerResource.addResource('{feed_id}');

    // DELETE /feeds/{feed_id} - Marks a feed as inactive or soft-deletes it
    singleFeedResource.addMethod('DELETE', feedManagerLambdaIntegration);

    // Create a resource for items of specific feeds: /feeds/{feed_id}/items
    const itemsResource = singleFeedResource.addResource('items');

    // GET /feeds/{feed_id}/items - Retrieves items for a specific feed
    itemsResource.addMethod('GET', feedManagerLambdaIntegration);

    new cdk.CfnOutput(this, 'FeedManagerApiUrl', {
      value: feedManagerApi.url
    });
  }
}
