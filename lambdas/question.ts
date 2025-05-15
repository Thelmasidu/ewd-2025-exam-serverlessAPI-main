import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  QueryCommand,
  QueryCommandInput,
} from "@aws-sdk/lib-dynamodb";

const client = createDDbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
  try {
    console.log("Event: ", JSON.stringify(event));

    const cinemaId = event.pathParameters?.cinemaId;
    const movieId = event.queryStringParameters?.movieId;
    const period = event.queryStringParameters?.period;

    if (!cinemaId) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing path parameter: cinemaId" }),
      };
    }

    const expressionValues: Record<string, any> = {
      ":cinemaId": Number(cinemaId),
    };

    const params: QueryCommandInput = {
      TableName: "CinemaTable",
      KeyConditionExpression: "",
      ExpressionAttributeValues: expressionValues,
    };

    if (movieId) {
      params.KeyConditionExpression = "cinemaId = :cinemaId AND movieId = :movieId";
      expressionValues[":movieId"] = movieId;
    } else if (period) {
      params.IndexName = "periodIx";
      params.KeyConditionExpression = "cinemaId = :cinemaId AND period = :period";
      expressionValues[":period"] = period;
    } else {
      params.KeyConditionExpression = "cinemaId = :cinemaId";
    }

    const result = await client.send(new QueryCommand(params));

    return {
      statusCode: 200,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(result.Items || []),
    };
  } catch (error: any) {
    console.error("Error:", JSON.stringify(error));
    return {
      statusCode: 500,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ error }),
    };
  }
};

function createDDbDocClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  return DynamoDBDocumentClient.from(ddbClient, {
    marshallOptions: {
      convertEmptyValues: true,
      removeUndefinedValues: true,
    },
    unmarshallOptions: {
      wrapNumbers: false,
    },
  });
}
