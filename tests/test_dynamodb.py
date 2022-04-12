from unittest import TestCase
from faker import Faker
from cb_dynamodb.dynamodb import DynamoDB


class DynamoDBLogicTests(TestCase):
    """API logic test suite"""

    def setUp(self) -> None:
        self.fake = Faker()
        Faker.seed()
        self.country = "chile"
        self.school_id = str(self.fake.uuid4())
        self.parent_id = "1"
        self.message = {
            "school_email": "mail@mail.cl",
            "school_id": self.school_id,
            "school_uuid": str(self.fake.uuid4()),
            "school_name": "School {} {}".format(
                self.fake.name(), self.fake.suffix()
            ),
            "school_contact": self.fake.name(),
            "contact_type": "parent",
            "position": "Director",
            "message_title": self.fake.sentence(nb_words=10),
            "message": self.fake.paragraph(nb_sentences=1),
            "country": self.country,
            "user_id": str(self.fake.uuid4()),
            "profile_id": str(self.fake.uuid4()),
            "username": self.fake.first_name(),
            "mail_from": "hola@hola",
            "phone": "+569123456789",
            "parent_id": self.parent_id,
        }

        self.dynamo_obj = DynamoDB(
            table_name="messages_staging", country=self.country
        )

    def __getattribute__(self, item):
        return super(DynamoDBLogicTests, self).__getattribute__(item)

    def test_dynamo_format_message_response_object_type(self) -> None:
        message_uuid, formatted_message = self.dynamo_obj.format_message(
            self.message
        )
        self.assertIsInstance(formatted_message, dict)

        for item in formatted_message.values():
            self.assertIsInstance(item["S"], str)

        message = {
            "school_email": "mail@mail.cl",
            "school_id": 22,
            "school_uuid": str(self.fake.uuid4()),
            "school_name": "School {} {}".format(
                self.fake.name(), self.fake.suffix()
            ),
            "school_contact": self.fake.name(),
            "contact_type": "parent",
            "position": "Director",
            "message_title": self.fake.sentence(nb_words=10),
            "message": self.fake.paragraph(nb_sentences=1),
            "country": self.country,
            "user_id": str(self.fake.uuid4()),
            "profile_id": str(self.fake.uuid4()),
            "username": self.fake.first_name(),
            "mail_from": "hola@hola",
            "phone": "+569123456789",
            "parent_id": self.parent_id,
        }
        id, response = self.dynamo_obj.format_message(message)
        self.assertIsInstance(response, dict)

    def test_dynamo_format_message_error(self) -> None:
        self.assertRaises(TypeError, self.dynamo_obj.format_message([]))
        self.assertRaises(
            TypeError, self.dynamo_obj.format_message(dict_to_format="")
        )
        self.assertRaises(TypeError, self.dynamo_obj.format_message(True))

    def test_dynamo_unformat_message_response_object_type(self) -> None:
        message_uuid, formatted_message = self.dynamo_obj.format_message(
            self.message
        )

        dictlist = []
        temp = {}
        for key, value in formatted_message.items():
            temp[key] = value
            dictlist.append(temp)
            temp = {}

        unformatted_message = self.dynamo_obj.unformat_message(dictlist)
        self.assertIsInstance(unformatted_message, list)

    def test_dynamo_unformat_message_errors(self) -> None:
        message_uuid, formatted_message = self.dynamo_obj.format_message(
            self.message
        )

        self.assertRaises(
            TypeError, self.dynamo_obj.unformat_message(formatted_message)
        )
        self.assertRaises(TypeError, self.dynamo_obj.unformat_message([]))

    def test_dynamo_successful_insertion_on_messages_table(self) -> None:
        response = self.dynamo_obj.post_message(message=self.message)
        self.assertEqual(response["ResponseMetadata"]["HTTPStatusCode"], 200)

    def test_dynamo_unsuccessful_insertion_on_messages_table(self) -> None:
        print("Hola")
        # response = self.dynamo_obj.post_message(2)
        # self.assertEqual(response["ResponseMetadata"]["HTTPStatusCode"], 200)

    def test_search_on_index(self) -> None:
        indexes_dict = self.dynamo_obj.get_index_secondary_indexes()
        indexes_names = indexes_dict.keys()
        self.dynamo_obj.post_message(message=self.message)
        for index in indexes_names:
            try:
                var = index.split("-index")[0]
                response = self.dynamo_obj.search_on_index(
                    index=index, value=self.__getattribute__(var)
                )
                self.assertEqual(self.__getattribute__(var), response[0][var])
                del response[0]["message_id"], response[0]["timestamp"]
                self.assertDictEqual(self.message.lower(), response[0])
            except AttributeError:
                pass

    def test_search_on_index_errors(self) -> None:
        with self.assertRaises(TypeError):
            self.dynamo_obj.search_on_index(index="blah", value=1)
            self.dynamo_obj.search_on_index(index="blah", value="")
            self.dynamo_obj.search_on_index(index="", value="")

        with self.assertRaises(TypeError):
            indexes_dict = self.dynamo_obj.get_index_secondary_indexes()
            indexes_names = indexes_dict.keys()
            self.dynamo_obj.search_on_index(
                index=list(indexes_names)[0].split("-index")[0], value=""
            )

        with self.assertRaises(FileNotFoundError):
            indexes_dict = self.dynamo_obj.get_index_secondary_indexes()
            indexes_names = indexes_dict.keys()
            self.dynamo_obj.search_on_index(
                index=list(indexes_names)[0].split("-index")[0], value="1"
            )

    def test_get_index_secondary_indexes(self) -> None:
        indexes_dict = self.dynamo_obj.get_index_secondary_indexes(
            table_name="messages_staging"
        )
        self.assertIsInstance(indexes_dict, dict)

    def test_get_index_secondary_indexes_errors(self) -> None:
        with self.assertRaises(FileNotFoundError):
            self.dynamo_obj.get_index_secondary_indexes(table_name="asdfghj")

        with self.assertRaises(AttributeError):
            self.dynamo_obj.get_index_secondary_indexes(table_name="")

    def test_get_all_messages_from_index(self) -> None:
        messages = self.dynamo_obj.get_all_messages_from_index()
        self.assertIsInstance(messages, list)

        messages = self.dynamo_obj.get_all_messages_from_index(
            table_name="messages_staging"
        )
        self.assertIsInstance(messages, list)

    def test_get_all_messages_from_index_errors(self) -> None:
        with self.assertRaises(FileNotFoundError):
            self.dynamo_obj.get_all_messages_from_index(table_name="adsfghj")
        with self.assertRaises(AttributeError):
            self.dynamo_obj.get_all_messages_from_index(table_name="")

    def test_get_index_count(self) -> None:
        index_count = self.dynamo_obj.get_index_count()
        self.assertIsInstance(index_count, int)

        index_count = self.dynamo_obj.get_index_count("messages_staging")
        self.assertIsInstance(index_count, int)

    def test_get_index_count_errors(self) -> None:
        with self.assertRaises(FileNotFoundError):
            self.dynamo_obj.get_index_count("sdfkj")

        with self.assertRaises(Exception):
            self.dynamo_obj.get_index_count(table_name="")

    def test_get_index_key_schema(self) -> None:
        schema = self.dynamo_obj.get_index_key_schema()
        self.assertIsInstance(schema, list)

        schema = self.dynamo_obj.get_index_key_schema("messages_staging")
        self.assertIsInstance(schema, list)

    def test_get_index_key_schema_error(self) -> None:
        with self.assertRaises(FileNotFoundError):
            self.dynamo_obj.get_index_key_schema(table_name="asdfgh")
        with self.assertRaises(AttributeError):
            self.dynamo_obj.get_index_key_schema(table_name="")
