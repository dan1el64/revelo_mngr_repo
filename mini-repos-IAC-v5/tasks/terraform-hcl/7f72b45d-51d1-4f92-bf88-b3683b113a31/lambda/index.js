const AWS = require('aws-sdk');

const sqs = new AWS.SQS();
const s3 = new AWS.S3();
const dynamodb = new AWS.DynamoDB();

exports.handler = async (event) => {
  const { action, requestId } = event;
  const timestamp = new Date().toISOString();
  
  if (action === 'enqueue') {
    const params = {
      QueueUrl: process.env.QUEUE_URL,
      MessageBody: JSON.stringify({
        timestamp,
        requestId
      })
    };
    
    await sqs.sendMessage(params).promise();
    return { statusCode: 200, body: 'Message enqueued' };
  }
  
  if (action === 'archive') {
    const params = {
      Bucket: process.env.BUCKET_NAME,
      Key: `events/${requestId}.json`,
      Body: JSON.stringify({
        timestamp,
        requestId
      })
    };
    
    await s3.putObject(params).promise();
    
    const dbParams = {
      TableName: process.env.TABLE_NAME,
      Item: {
        pk: { S: requestId },
        expiresAt: { N: `${Math.floor(Date.now() / 1000) + 86400}` }
      },
      ConditionExpression: 'attribute_not_exists(pk)'
    };
    
    try {
      await dynamodb.putItem(dbParams).promise();
      return { statusCode: 200, body: 'Event archived' };
    } catch (error) {
      if (error.name === 'ConditionalCheckFailedException') {
        return { statusCode: 409, body: 'Duplicate requestId' };
      }
      throw error;
    }
  }
  
  return { statusCode: 400, body: 'Invalid action' };
};