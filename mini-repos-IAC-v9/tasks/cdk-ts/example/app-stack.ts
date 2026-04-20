import { Stack, StackProps, CfnOutput, Tags } from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';

interface AppStackProps extends StackProps {
  namePrefix: string;
}

export class AppStack extends Stack {
  constructor(scope: Construct, id: string, props: AppStackProps) {
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
