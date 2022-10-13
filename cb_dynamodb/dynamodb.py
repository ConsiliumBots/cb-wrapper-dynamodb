import datetime
import decimal
from functools import reduce
import uuid
import math
from typing import Union
import warnings
from ast import literal_eval
import boto3
import botocore
import botocore.exceptions
import botocore.errorfactory
from boto3.dynamodb.conditions import Key, Attr, And
from boto3.dynamodb.types import TypeDeserializer
from botocore.errorfactory import (
    ClientError,
    BaseClientExceptions,
    ClientExceptionsFactory,
)

from cb_dynamodb.utils import set_logger

from dataclasses import dataclass, field, InitVar

log = set_logger(name=__name__, level="debug")


class DynamoDB:
    """
    A class used to represent a DynamoDB object
    ...
    Attributes
    ----------
    table_name : str
        a formatted string with DynamoDB table name
        to interact with
    country : str
        a formatted string with the country name needed
        for search methods (default None)
    region_name : str
        a formatted string with AWS region name where
        table is located (default us-east-1)
    Methods
    -------
    format_message(self, dict_to_format: dict,
    type_of_action=None, timestamp=None)
        Returns a tuple with message_uuid and
        a formatted message ready to be
        post to a dynamo db table
    unformat_message(self, to_unformat: list)
        Unformat a message with DynamoDB format
        and returns a dictionary
    post_message(self, message: dict, timestamp=None):
        Post message to the DynamoDB table of the
        instanciated class
    get_all_messages_from_index(self, table_name=None)
        Returns all messages from a table
    get_index_count(self, table_name=None)
        Returns the number of items of a table
    get_index_key_schema(self, table_name=None)
        Returns the index key schema of a table
    get_index_secondary_indexes(self, table_name=None)
        Returns the secondary indexes of a table
    search_on_index(self, index: str, value: str)
        Returns all the items on the table of the instanciated class
        who match with the index and value provided
    upload_table(self, index_name, data_to_upload, timestamp=None)
        Allows to upload data the a table
    truncate_table(self)
        Truncate the table of the instanciated class
    """

    def __init__(self, table_name: str, country=None, region_name="us-east-1"):
        """
        :param table_name: Name of DynamoDB table
        :type table_name: str
        :param country: Name of the country
        :type country: str
        :param region_name: Name of the region
        :type region_name: str
        """
        self.client = boto3.client(
            "dynamodb",
            region_name=region_name,
        )
        self.dynamo_resource = boto3.resource("dynamodb")
        self.table_name = table_name
        if country == "newhaven":
            self.country = "new haven"
        else:
            self.country = country

    def __repr__(self):
        rep = f"Dynamo({self.table_name})"
        return rep

    def __str__(self):
        str = f"{self.table_name}"
        return str

    def format_message(
        self, dict_to_format: dict, type_of_action=None, timestamp=None
    ) -> tuple:
        """
        Returns a tuple with message_uuid and a formatted message ready to be
        post to a dynamo db table.
        :param dict_to_format: Dictionary
        :type dict_to_format: dict
        :param type_of_action: Type of message to format. If it is "put",
        a message_id and a timestamp will be added.
        :type type_of_action: str, optional
        :param timestamp: Message timestamp (Default is the current
        date and time)
        :type timestamp: Timestamp, optional
        :return: Message uuid and the formatted message
        :rtype: tuple
        """
        message_uuid = str(uuid.uuid4())
        if type_of_action == "put":
            formatted_message = {
                "message_id": {"S": message_uuid},
                "timestamp": {
                    "S": str(datetime.datetime.now())
                    if timestamp is None
                    else timestamp
                },
            }
        else:
            formatted_message = {
                "timestamp": {
                    "S": str(datetime.datetime.now())
                    if timestamp is None
                    else timestamp
                }
            }
        try:
            for key in dict_to_format.keys():
                formatted_message[key] = {}
                formatted_message[key]["S"] = (
                    ""
                    if dict_to_format[key] is None
                    else str(dict_to_format[key]).lower()
                )
        except (AttributeError, TypeError) as error:
            log.error(
                error,
                {
                    "index_meta": True,
                    "table_name": self.table_name,
                },
            )
        return message_uuid, formatted_message

    def unformat_message(self, to_unformat: list) -> list:
        """
        Unformat a message with DynamoDB format and returns a dictionary
        :param to_unformat: List with raw message from dynamo DB
        :type to_unformat: list
        :return: A list with the unformatted message
        :rtype: list
        """
        try:
            td = TypeDeserializer()
            unformatted_message = [
                {k: td.deserialize(v) for k, v in item.items()}
                for item in to_unformat
            ]
        except (AttributeError, TypeError) as error:
            log.error(
                error,
                {
                    "index_meta": True,
                    "table_name": self.table_name,
                },
            )
        except Exception as error:
            log.error(
                error,
                {
                    "index_meta": True,
                    "table_name": self.table_name,
                    "country": self.country,
                },
            )
        return unformatted_message

    def post_message(
        self, message: dict, timestamp=None, include_message_id=True
    ):
        """
        Post message to the DynamoDB table of the instanciated class
        :param message: Dictionary to be sent to a DynamoDB table
        :type message: dict
        :param timestamp: Date and time to be added to the message
        :type timestamp: Timestamp, optional (Default: current datetime)
        :param include_message_id: True if you want to include a
        message_id into
        dynamo db post. Default is True
        :type include_message_id: Bool
        :return: Returns a DynamoDB response
        :rtype: response
        """
        try:
            if include_message_id:
                message_uuid, formatted_message = self.format_message(
                    dict_to_format=message,
                    type_of_action="put",
                    timestamp=timestamp,
                )
            else:
                message_uuid, formatted_message = self.format_message(
                    dict_to_format=message,
                    timestamp=timestamp,
                )

            response = self.client.put_item(
                TableName=self.table_name,
                Item=formatted_message,
            )

            log.info(
                f"Message {message_uuid} saved successfully "
                f"on table {self.table_name}",
                {
                    "index_meta": True,
                    "table_name": self.table_name,
                    "country": self.country,
                    "message_uuid": message_uuid,
                },
            )
            return response
        except botocore.exceptions.ClientError as error:
            raise FileNotFoundError(error)
        except Exception as error:
            log.error(
                error,
                {
                    "index_meta": True,
                    "table_name": self.table_name,
                    "country": self.country,
                },
            )
            raise Exception

    def get_all_messages_from_index(self, table_name=None) -> list:
        """
        Returns all messages from a table
        :param table_name: Table name to be scan
        :type table_name: str, optional (Default is tablename of the
        instanciated class)
        :return: A list with all the elements of the table requested
        :rtype: list
        """
        try:
            if table_name is None:
                response = self.client.scan(TableName=self.table_name)
            else:
                response = self.client.scan(TableName=table_name)

            return self.unformat_message(response["Items"])
        except Exception as error:
            log.error(
                error,
                {
                    "index_meta": True,
                    "table_name": self.table_name,
                    "country": self.country,
                },
            )
            if error.response["Error"]["Code"] == "ResourceNotFoundException":
                raise FileNotFoundError from error
            elif error.response["Error"]["Code"] == "ParamValidationError":
                raise AttributeError from error
            else:
                raise Exception

    def get_column_count(self, index: str, value: str):
        """
        Returns the number of items of a table
        :param table_name: Table name to be scan
        :type table_name: str, optional (Default is tablename of the
        instanciated class)
        :return: A integer with the number of items of the table requested
        :rtype: int
        """
        try:
            table = self.dynamo_resource.Table(self.table_name)
            query = table.query(
                IndexName=f"{index}-index",
                Select="COUNT",
                KeyConditionExpression=Key(index).eq(value),
            )
            return query["ScannedCount"]
        except ClientError as error:
            raise SystemError(error)
        except Exception as error:
            raise Exception(error)

    def get_index_count(self, table_name=None) -> int:
        """
        Returns the number of items of a table
        :param table_name: Table name to be scan
        :type table_name: str, optional (Default is tablename of the
        instanciated class)
        :return: A integer with the number of items of the table requested
        :rtype: int
        """
        try:
            if table_name is None:
                table = self.dynamo_resource.Table(self.table_name)
            else:
                table = self.dynamo_resource.Table(table_name)
            return table.item_count
        except botocore.errorfactory.ClientError as error:
            log.error(
                error,
                {
                    "index_meta": True,
                    "table_name": table_name,
                    "country": self.country,
                    "error": error.response["Error"]["Code"],
                    "status": error.response["ResponseMetadata"][
                        "HTTPStatusCode"
                    ],
                },
            )
            if error.response["Error"]["Code"] == "ResourceNotFoundException":
                raise FileNotFoundError(error)
            elif error.response["Error"]["Code"] == "ParamValidationError":
                raise AttributeError(error)
            else:
                raise botocore.errorfactory.ClientError
        except ValueError as error:
            log.error(
                error,
                {
                    "index_meta": True,
                    "table_name": table,
                    "country": self.country,
                },
            )
            raise AttributeError(error)
        except Exception as error:
            log.error(
                error,
                {
                    "index_meta": True,
                    "table_name": table_name,
                    "country": self.country,
                },
            )
            raise Exception

    def get_index_key_schema(self, table_name=None) -> str:
        """
        Returns the index key schema of a table
        :param table_name: Table name to be scan
        :type table_name: str, optional (Default is tablename of the
        instanciated class)
        :return: Key index of the table requested
        :rtype: str
        """
        try:
            if table_name is None:
                table = self.dynamo_resource.Table(self.table_name)
            else:
                table = self.dynamo_resource.Table(table_name)
            return table.key_schema
        except Exception as error:
            log.error(
                error,
                {
                    "index_meta": True,
                    "table_name": table,
                    "country": self.country,
                },
            )
            if error.response["Error"]["Code"] == "ResourceNotFoundException":
                raise FileNotFoundError from error
            elif error.response["Error"]["Code"] == "ParamValidationError":
                raise AttributeError from error
            else:
                raise Exception

    def get_index_secondary_indexes(self, table_name=None) -> dict:
        """
        Returns the secondary indexes of a table
        :param table_name: Table name to be scan
        :type table_name: str, optional (Default is tablename of the
        instanciated class)
        :return: A dictionary with all the secondary indexes of the table
        requested
        :rtype: list
        """
        try:
            if table_name is None:
                table = self.dynamo_resource.Table(self.table_name)
            else:
                table = self.dynamo_resource.Table(table_name)

            secondary_indexes = table.global_secondary_indexes

            response = {}
            for secondary_index in secondary_indexes:
                response[secondary_index["IndexName"]] = secondary_index[
                    "KeySchema"
                ]
            return response
        except Exception as error:
            log.error(
                error,
                {
                    "index_meta": True,
                    "table_name": table,
                    "country": self.country,
                },
            )
            if error.response["Error"]["Code"] == "ResourceNotFoundException":
                raise FileNotFoundError(error) from error
            elif error.response["Error"]["Code"] == "ParamValidationError":
                raise AttributeError(error) from error
            else:
                raise Exception from error

    def search_on_index(self, index: str, value: str) -> list:
        """
        Returns all the items on the table of the instanciated class
        who match with the index and value provided
        :param index: Index of the table
        :type index: str
        :param value: Value to be search
        :type value: str
        :return: A list with all the items found
        :rtype: list
        """
        if "-index" in index:
            index = index.split("-")[0]
        if value == "new+haven":
            value = "new haven"

        try:
            if index == "country":
                response = self.client.query(
                    TableName=self.table_name,
                    IndexName=f"{index}-index",
                    KeyConditionExpression=f"{index} = :p",
                    ExpressionAttributeValues={":p": {"S": str(value)}},
                )
            if self.country is None:
                response = self.client.query(
                    TableName=self.table_name,
                    IndexName=f"{index}-index",
                    KeyConditionExpression=f"{index} = :p",
                    ExpressionAttributeValues={":p": {"S": str(value)}},
                )
            else:
                response = self.client.query(
                    TableName=self.table_name,
                    IndexName=f"{index}-index",
                    KeyConditionExpression=f"{index} = :p",
                    ExpressionAttributeValues={
                        ":p": {"S": str(value)},
                        ":c": {"S": self.country},
                    },
                    FilterExpression="country = :c",
                )
            if response["Count"] == 0:
                raise FileNotFoundError
            return self.unformat_message(response["Items"])
        except botocore.exceptions.ParamValidationError as error:
            log.error(
                error,
                {
                    "index_meta": True,
                    "table_name": self.table_name,
                    "country": self.country,
                    "error": error,
                },
            )
            raise TypeError
        except botocore.exceptions.ClientError as error:
            log.error(
                error,
                {
                    "index_meta": True,
                    "table_name": self.table_name,
                    "index": index,
                    "value": value,
                    "error": error.response["Error"]["Code"],
                    "status": error.response["ResponseMetadata"][
                        "HTTPStatusCode"
                    ],
                },
            )
            if error.response["Error"]["Code"] == "ValidationException":
                raise TypeError(error) from error
            elif (
                error.response["Error"]["Code"] == "ResourceNotFoundException"
            ):
                raise FileNotFoundError from error
            else:
                raise botocore.exceptions.ClientError

    def upload_table(self, data_to_upload: str, timestamp=None):
        """
        Allows to upload data the a table
        :param data_to_upload: Path to the file to be uploaded
        :type data_to_upload: str
        :param timestamp: Timestamp to be added to the messages
        :type timestamp: Timestamp
        :return: A string with the number of items uploaded
        :rtype: str
        """
        # todo: testing
        counter = 0
        # todo: automatizar keys en blanco
        # todo si index_name no viene blanco, guardar en esa tabla
        for item in data_to_upload["_source"]:
            item_json = literal_eval(item)
            self.post_message(item_json, timestamp=timestamp)
            counter = counter + 1
        return f"{counter} items uploaded", 200

    def truncate_table(self):
        """
        Truncate the table of the instanciated class
        :return: None
        :rtype: None
        """
        # todo: testing
        index_count = self.get_index_count()
        step = 25
        start = 0
        finish = start + step
        items_to_delete = []

        for _ in range(math.ceil(index_count / step)):
            all_items = self.get_all_messages_from_index()
            for item in all_items[start:finish]:
                items_to_delete.append(
                    {
                        "DeleteRequest": {
                            "Key": {
                                "message_id": item["message_id"],
                                "timestamp": item["timestamp"],
                            }
                        }
                    }
                )
            try:
                self.client.batch_write_item(
                    RequestItems={self.table_name: items_to_delete}
                )
            except botocore.exceptions.ParamValidationError:
                pass
            start = finish
            finish = finish + step
            items_to_delete = []


@dataclass
class Dynamo:
    """
    Dynamo resource class representation
    """

    db = boto3.resource("dynamodb")
    country: str = None
    region_name: str = "us-east-1"


class DynamoPost(Dynamo):
    """
    Post actions over Dynamo Table
    """

    @classmethod
    def post_item(
        cls,
        table_name: str,
        message: dict,
    ):
        """
        Create a new item on the specified table
        :param table_name: Table name
        :type table_name: str
        :param message: Information to post
        :type message: dict
        :return: True if the item was successfully created
        :rtype: Bool
        """
        try:
            table = cls.db.Table(f"{table_name}")
            response = table.put_item(Item=message)
            log.info(
                f"Message {response['ResponseMetadata']['RequestId']} saved successfully "
                f"on table {table}",
                {
                    "index_meta": True,
                    "table_name": table,
                    "country": cls.country,
                    "message_uuid": 1,
                },
            )
            return True
        except botocore.errorfactory.ClientError as error:
            log.error(
                error,
                {
                    "index_meta": True,
                    "table": table_name,
                    "country": cls.country,
                    "error_code": error.__dict__["response"][
                        "ResponseMetadata"
                    ]["HTTPStatusCode"],
                    "message": error.__dict__["response"]["Error"]["Message"],
                },
            )
            raise error
        except decimal.Inexact as error:
            log.error(
                f"decimal.Inexact: {error}. Trye ",
                {
                    "index_meta": True,
                    "table": table_name,
                    "country": cls.country,
                },
            )
            raise Exception(error)
        except Exception as error:
            log.error(
                error,
                {
                    "index_meta": True,
                    "table": table_name,
                    "country": cls.country,
                },
            )
            raise error


class DynamoSearch(Dynamo):
    """
    Search actions over a dynamo table
    """

    @classmethod
    def get_item(cls, table_name:str, key:Union[dict, str]) -> dict:
        """
        Obtains an item from the specified
        table
        :param table_name: Table name
        :type table_name: str
        :param key: Dictionary with the partition
        key to search; if string, then it represents
        partition key value
        :type key: dict with Partition Key, Sort key (Optional),
        or string with partition key value
        :return: Object dictionary
        :rtype: dict
        """
        try:
            table = cls.db.Table(table_name)
            if isinstance(key, str):
                key = {table.key_schema.pop()['AttributeName']: key}
            response = table.get_item(Key=key)
            item = response.get("Item")
            if item is None:
                print(f"{key} not found on table {table_name}")
            return item
        except ClientError as e:
            raise NotImplementedError

    @classmethod
    def get_column(cls, table_name:str, attr_name:str, filters:dict=None) -> list:
        """
        Get values column of common attribute
        for all items in specified table
        :param table_name: Table name
        :type table_name: str
        :param attr_name: Name of attribute to get
        :type attr_name: str
        :param filters: Optional filter dictionary
        :type filters: dict
        :return: List of dicts were keys are (primary_key_name, attr_name)
        and values are the corresponding values of each item
        :rtype: list
        """
        try:
            table = cls.db.Table(table_name)
            table_key = table.key_schema.pop()['AttributeName']
            if filters:
                response = table.scan(
                    ProjectionExpression = f'{table_key}, {attr_name}',
                    FilterExpression=reduce(And, ([Key(k).eq(v) for k, v in filters.items()]))
                )
                data = response['Items']
                # Handle large responses (DDB limits scan to 1mb)
                while response.get('LastEvaluatedKey'):
                    response = table.scan(
                        ProjectionExpression = f'{table_key}, {attr_name}',
                        FilterExpression=reduce(And, ([Key(k).eq(v) for k, v in filters.items()])),
                        ExclusiveStartKey=response['LastEvaluatedKey']
                    )
                    data.extend(response['Items'])
            else:
                response = table.scan(
                    ProjectionExpression = f'{table_key}, {attr_name}'
                )
                data = response['Items']
                # Handle large responses (DDB limits scan to 1mb)
                while response.get('LastEvaluatedKey'):
                    response = table.scan(
                        ProjectionExpression = f'{table_key}, {attr_name}',
                        ExclusiveStartKey=response['LastEvaluatedKey']
                    )
                    data.extend(response['Items'])
            # Check length of response
            len_items = [len(item) for item in data]
            if len(len_items) == 0:
                raise AttributeError(f"{table} contains no items that match these criteria")
            elif max(len_items) == 1:
                raise AttributeError(f"{table} object has no attribute '{attr_name}'")
            elif min(len_items) == 1:
                warnings.warn(f"some items are missing the '{attr_name}' attribute")
        except ClientError:
            raise NotImplementedError
        else:
            return data
