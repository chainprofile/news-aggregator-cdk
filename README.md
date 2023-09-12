# Welcome to your CDK TypeScript project

This is a blank project for CDK development with TypeScript.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

## Useful commands

* `npm run build`   compile typescript to js
* `npm run watch`   watch for changes and compile
* `npm run test`    perform the jest unit tests
* `cdk deploy`      deploy this stack to your default AWS account/region
* `cdk diff`        compare deployed stack with current state
* `cdk synth`       emits the synthesized CloudFormation template


## Features Available:

1. Ability to add feed_urls to fetch the feed items
2. Scheduler to schedule fetching tasks based on informatin available in the feed channel(feed polling)
3. De-duplication support to avoid storing feed items that are already available.

## Todo

1. Support for real time access to feed items using WebSub(Previously PubSubHubbub).
2. Code cleanup
3. Better logging and error handling.
4. Add tests for lambda functions and cdk 
