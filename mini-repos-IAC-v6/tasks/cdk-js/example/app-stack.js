const { Stack, CfnOutput, Tags } = require('aws-cdk-lib');
const s3 = require('aws-cdk-lib/aws-s3');

class AppStack extends Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    const { namePrefix } = props;

    const bucket = new s3.Bucket(this, 'Main', {
      bucketName: `${namePrefix}-bucket`,
    });

    Tags.of(bucket).add('Name', `${namePrefix}-bucket`);
    Tags.of(bucket).add('Project', namePrefix);

    new CfnOutput(this, 'BucketName', {
      value: bucket.bucketName,
      description: 'S3 bucket name',
    });
  }
}

module.exports = { AppStack };
