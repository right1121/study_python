import json
import uuid

import boto3


class rds_db():
    table_column_data = {
        "business": {
            "cpid": {
                "type": "stringValue"
            },
            "business_name": {
                "type": "stringValue"
            },
            "docomokouza_number": {
                "type": "stringValue"
            },
            "access_key": {
                "type": "stringValue"
            },
            "secret_access_key": {
                "type": "stringValue"
            },
        }
    }

    def __init__(self, cluster_arn, secret_arn, data_base):
        self.__cluster_arn = cluster_arn
        self.__secret_arn = secret_arn
        self.__data_base = data_base
        self.__client = boto3.client("rds-data")

    def insert_db(
        self,
        table_name,
        insert_data
    ):
        column = rds_db.table_column_data[table_name]

        sql = self.generate_insert_sql(
            column.keys(), table_name)

        sql_parameters = self.generate_sql_parameters(
            column, insert_data)

        res = self.execute_sql(sql, sql_parameters)
        return res

    def generate_insert_sql(self, column_list, table_name):
        """INSERのSQL文を作成
        """
        column_names = ", ".join(column_list)
        values = ":" + ", :".join(column_list)

        sql = f"INSERT INTO {table_name} ({column_names}) VALUE ({values})"

        return sql

    def generate_sql_parameters(self, column_list, data):
        """SQLパラメータセット作成

        batch_execute_statementのparameterSetsを作成する
        """
        parameter_list = []
        for column_name, column_data in column_list.items():
            value = data[column_name]

            if value is None:
                column_type = "isNull"
                value = True
            else:
                column_type = column_data["type"]

            parameter_list.append(
                {
                    "name": column_name,
                    "value": {
                        column_type: value
                    }
                }
            )

        return parameter_list

    def select_db(
        self,
        table_name,
        column_list,
        join_table_name=None,
        join_condition=None,
        where=None,
        sort_key_list=None,
        sort="DESC",
            convert_type="DICT"):
        """パラメータをもとにSQL作成を行いDB検索
            辞書型の配列にて返却

            convert_type: レスポンスフォーマット
        """
        convert_type_list = {
            "DICT": self.convert_db_result_into_dict_type,
            "SQS": self.convert_db_result_into_sqs_type,
        }
        convert_type_keys = list(convert_type_list.keys())
        if convert_type not in convert_type_keys:
            raise ValueError(f"引数は次のいずれかです。:{convert_type_keys}")

        sql = self.generate_select_sql(
            table_name,
            column_list,
            join_table_name,
            join_condition,
            where,
            sort_key_list,
            sort)

        record_list = self.execute_sql(sql)

        convert_fnc = convert_type_list[convert_type]
        converted_record_list = convert_fnc(
            record_list, column_list)

        return converted_record_list

    def generate_select_sql(
        self,
        table_name, column_list,
        join_table_name=None,
        join_condition=None,
        where=None,
        sort_key_list=None,
            sort=None):
        """パラメーターからSQL(SELECT)を作成
            sortKeyはdefault昇順
        """
        column = ", ".join(column_list)
        sql_ = f"SELECT {column} FROM {table_name}"

        if join_table_name is not None:
            sql_ = sql_ + f" JOIN {join_table_name} ON {join_condition}"

        if where is not None:
            sql_ = sql_ + f" WHERE {where}"

        if sort_key_list is not None:
            sort_key = ", ".join(sort_key_list)
            sql_ = sql_ + f" ORDER BY {sort_key}" + f" {sort}"

        return sql_

    def convert_db_result_into_dict_type(self, record_list, column_list):
        """DB検索結果を辞書型に変換
        """
        formated_record_list = []
        for record in record_list.get("records", []):
            formated_record_data = {}

            for i, colums in enumerate(record):
                key = column_list[i]

                if list(colums.keys())[0] == "isNull":
                    value = ""
                else:
                    value = list(colums.values())[0]

                formated_record_data[key] = value

            formated_record_list.append(formated_record_data)

        return formated_record_list

    def convert_db_result_into_sqs_type(self, record_list, column_list):
        """DB検索結果をSQS送信フォーマットに変換
        """
        msg_group_id = uuid.uuid4()
        formated_record_list = []

        for i, record in enumerate(record_list["records"]):

            formated_record_data = {
                "Id": str(i + 1),
                "MessageGroupId": str(msg_group_id)
            }
            message_body = {}

            for j, colums in enumerate(record):
                sql_select_name = column_list[j]

                # 結合条件のテーブル名付与箇所を削除
                key = sql_select_name.split(".")[-1]

                if list(colums.keys())[0] == "isNull":
                    value = ""
                else:
                    value = list(colums.values())[0]

                message_body[key] = value
            formated_record_data["MessageBody"] = json.dumps(message_body)

            formated_record_list.append(formated_record_data)

        return formated_record_list

    def execute_sql(self, sql_, sql_parameters=None):
        """SQL実行
        """
        parameters = {
            "secretArn": self.__secret_arn,
            "database": self.__data_base,
            "resourceArn": self.__cluster_arn,
            "sql": sql_,
        }
        if sql_parameters is not None:
            parameters["parameters"] = sql_parameters

        res = self.__client.execute_statement(**parameters)
        return res


def get_parameter_store():
    """パラメータストアからデータ取得
    """
    # DB系
    cluster_arn = get_ssm_parameters(cluster_param)
    secret_arn = get_ssm_parameters(secret_param)
    data_base = get_ssm_parameters(data_base_param)

    return cluster_arn, secret_arn, data_base


def get_ssm_parameters(param_key):
    """パラメータストアから定数取得
    """
    response = ssm_client.get_parameters(
        Names=[
            param_key
        ],
        WithDecryption=True
    )
    if len(response["InvalidParameters"]) != 0:
        raise Exception("パラメータストアに値がありません。")
    return response["Parameters"][0]["Value"]


if __name__ == "__main__":

    ssm_client = boto3.client("ssm")

    cluster_param = "/BtoC/Lambda/arn/cluster"
    secret_param = "/BtoC/Lambda/arn/secret"
    data_base_param = "/BtoC/Database"
    cluster_arn, secret_arn, data_base = get_parameter_store()
    db = rds_db(cluster_arn, secret_arn, data_base)

    table_name = "business"
    data = {
        "cpid": "09999999990",
        "business_name": "test",
        "docomokouza_number": "123456789012",
        "access_key": "access_key",
        "secret_access_key": "secret_access_key",
    }

    # Create(INSERT)
    db.insert_db(
        table_name,
        data
    )

    # Read(SELECT)
    column_list = [
        "cpid",
        "business_name"
    ]
    res = db.select_db(
        table_name,
        column_list,
        convert_type="SQS"
    )
    print(res)
