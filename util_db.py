# -*- coding: utf-8 -*-
"""使い方- token/にsheetのjson

from util_db import Temple
tm = Temple(cnm='ruru')
tem_ple = tm._get('Temple)

tm.del_id(site='pc')

-------------

from util_db import UserModel, LogModel

user = UserModel(user_id, cnm)
user.profile = EMAIL
#user.keys() =  [user_id, cnm, name, profile, email, message, hajime, asiato, meruado_otosi, view]

log = LogModel()
log(login_id, time=datetime.datetime.now())
#lgm.keys() = [login_id, time, user_id, act]
"""
import sys

sys.path.append(r'/home/ec2-user/.local/lib/python3.7/site-packages/')
import datetime
import json
import os
import pdb
import pickle
from collections import defaultdict

import boto3
import gspread
import pynamodb
import pandas as pd
#from dynamo_pandas import put_df, get_df, keys
import pysnooper
from boto3.dynamodb.conditions import Attr, Key
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from pynamodb.attributes import (JSONAttribute, ListAttribute, NumberAttribute,
                                DynamicMapAttribute, UnicodeAttribute,
                                UTCDateTimeAttribute, BooleanAttribute)
from pynamodb.models import Model
from pynamodb_utils import DynamicMapAttribute, AsDictModel, JSONQueryModel, TimestampedModel
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection, LocalSecondaryIndex
from datetime import timezone, datetime
from pynamodb_utils.utils import get_timestamp, parse_attrs_to_dict
from tinydb import Query, TinyDB, operations, where
import random
from glob import glob


import configparser

# --------------------------------------------------
# configparserの宣言とiniファイルの読み込み
# --------------------------------------------------
conf = configparser.ConfigParser()
conf.read('config.ini', encoding='utf-8')

# --------------------------------------------------
# config,iniから値取得
# --------------------------------------------------

ACCESS_KEY = conf['AWS']['ACCESS_KEY']
SECRET_KEY = conf['AWS']['SECRET_KEY']
REGION_NAME = conf['AWS']['REGION_NAME']
PEM_KEY = conf['AWS']['PEM_KEY']


#ななえ DATAシート
SHEET_KEY = conf['SHEET']['SHEET_KEY']


class MySheet:
    def __init__(self, file_path, worksheet, sheetname):
        gc = gspread.service_account(file_path)
        self.sh = gc.open(worksheet)
        self.wks = self.sh.worksheet(sheetname)

    def get_df(self):
        df = get_as_dataframe(self.wks, skiprows=0, header=0)
        df.dropna(how='all', inplace=True)
        return df

    def set_df(self, df):
        set_with_dataframe(self.wks, df)

    def set_df_ner(self, sheetname, df):
        worksheet = self.sh.add_worksheet(title=sheetname)
        set_with_dataframe(worksheet, df)


class Temple:

    def __init__(self, cnm):
        self.cnm = cnm
        tokens = glob('token/*sheet.json')
        file_path = random.choice(tokens)
        gc = gspread.service_account(file_path)
        self.sh = gc.open_by_key(SHEET_KEY)
        #self.tem_ple = self._get('Temple')

    @pysnooper.snoop()
    def _get(self, sheetname):
        cnm = self.cnm
        wks = self.sh.worksheet(sheetname)
        df = get_as_dataframe(wks, skiprows=0, header=0)
        df.dropna(subset=['cnm'], inplace=True)
        df = df.set_index('cnm', drop=False)
        d_dict = df.T.to_dict()
        return d_dict[cnm]

    @pysnooper.snoop()
    def del_id(self, site):
        """
        site名指定でID削除

        Args:
            site (str): pc,hp,ik,jm
        """
        cnm = self.cnm
        print(cnm, site)
        wks = self.sh.worksheet('Temple')
        df = get_as_dataframe(wks, index='cnm', skiprows=0, header=0)
        df = pd.DataFrame(df)
        df.dropna(how='all', inplace=True)
        df = df.set_index('cnm', drop=False)
        df.at[cnm, site] = ""

        set_with_dataframe(wks, df)


class MyDB:

    def __init__(self, table_name):
        self.tablename = table_name
        dynamodb = boto3.resource('dynamodb')
        self.table = dynamodb.Table(table_name)


    @classmethod
    def tables(cls):
        dynamodb = boto3.resource('dynamodb')
        table_list = dynamodb.tables.all()
        for table in table_list:
            print(table.table_name)

    def rm(self, key_dict):
        self.table.delete_item(Key=key_dict)

    def put(self, item_dict):
        res = self.table.put_item(
            Item=item_dict
        )
        return res

    def get(self, key_dict):
        items = self.table.get_item(
                    Key=key_dict
                )
        return items['Item'] if 'Item' in items else None

    # Partition Key（SerialNumber）での絞込検索
    def query(self, key, val):
        response = self.table.query(
            KeyConditionExpression=Key(key).eq(val)
        )
        return response['Items']

    def get_records(self, **kwargs):
        while True:
            response = self.table.scan()
            for item in response['Items']:
                yield item
            if 'LastEvaluatedKey' not in response:
                break
            kwargs.update(ExclusiveStartKey=response['LastEvaluatedKey'])

    def scan(self):
        records = self.get_records(self.table)
        return records

    def update_item(self, key_dict, update_key, update_val):

        res = self.table.update_item(
            Key = key_dict,
            AttributeUpdates = {'flag':{'Action': 'PUT', update_key: update_val}},
            ReturnValues = 'UPDATED_NEW'
        )
        return res


def value2none(item_dict):
    for k, v in item_dict.items():
        if not v:
            item_dict[k] = None
            print("{0} change None".format(k))

    return item_dict

class IdTimeIndex(GlobalSecondaryIndex):
    """
    This class represents a global secondary index
    index_name = 'pc-user-index'
    """
    class Meta:
        # index_name is optional, but can be provided to override the default name
        index_name = 'LoginId_UpdatedAt_Index'
        region = 'ap-northeast-1'
        TZINFO = timezone.utc
        read_capacity_units = 5
        write_capacity_units = 5
        # All attributes are projected
        projection = AllProjection()
    # This attribute is the hash key for the index
    # Note that this attribute must also exist
    # in the model
    login_id = UnicodeAttribute(default="0", hash_key=True)
    updated_at = UTCDateTimeAttribute(default=get_timestamp, range_key=True)

class TimeIndex(LocalSecondaryIndex):
    """
    This class represents a global secondary index
    """
    class Meta:
        # index_name is optional, but can be provided to override the default name
        index_name = 'time_index'
        region = 'ap-northeast-1'
        TZINFO = timezone.utc
        read_capacity_units = 5
        write_capacity_units = 5
        # All attributes are projected
        projection = AllProjection()
    # This attribute is the hash key for the index
    # Note that this attribute must also exist
    # in the model
    user_id = UnicodeAttribute(hash_key=True)
    updated_at = UTCDateTimeAttribute(default=get_timestamp, range_key=True)

class UserModel(AsDictModel, JSONQueryModel, TimestampedModel):
    """pcmax user data.
    TimestampedModelを引数に入れると
    created_at,updated_at,deleted_atが key に追加"""

    class Meta:
        table_name = "pc_user"
        region = 'ap-northeast-1'
        TZINFO = timezone.utc
    user_id = UnicodeAttribute(hash_key=True)
    cnm = UnicodeAttribute(range_key=True)
    
    login_id = UnicodeAttribute(default="0")
    updated_at = UTCDateTimeAttribute(default=get_timestamp)
    time_index = TimeIndex()
    
    profile = DynamicMapAttribute(default=dict)
    email = UnicodeAttribute(null=True)
    name = UnicodeAttribute(null=True)

    
    hajime = BooleanAttribute(null=False, default=False)
    asiato = BooleanAttribute(null=False, default=False)
    meruado = BooleanAttribute(null=False, default=False)
    gmail = BooleanAttribute(null=False, default=False)
    
    message = ListAttribute(default=list)
    
    view = NumberAttribute(default=0)



class AccountModel(AsDictModel, TimestampedModel):
    """アカウントの状態をチェック"""
    class Meta:
        table_name = "Account"
        region = 'ap-northeast-1'
        TZINFO = timezone.utc
    
    user_id = UnicodeAttribute(hash_key=True)
    cnm = UnicodeAttribute(range_key=True)
    #idx,key,valのdict の list
    message = ListAttribute(default=list)
    time = UTCDateTimeAttribute(default=datetime.now())


class LogModel(Model):
    """
    lg = LogModel(login_id, time, user_id=user_id, act=act, cnm=cnm)
    lg.save()
    """
    class Meta:
        table_name = "log"
        region = 'ap-northeast-1'
        #read_capacity_units = 5
        #write_capacity_units = 5
        TZINFO = timezone.utc

    login_id = UnicodeAttribute(hash_key=True)
    time = UTCDateTimeAttribute(range_key=True, default=datetime.now())
    user_id = UnicodeAttribute(null=True)
    cnm = UnicodeAttribute(null=True)
    act = UnicodeAttribute(null=True)
    


if __name__ == '__main__':
    """cnm = 'mika'
    site = 'pc'
    tm = Temple(cnm)
    sheetname = 'Temple'
    wks = tm.sh.worksheet(sheetname)
    df = get_as_dataframe(wks, skiprows=0, header=0)"""
    
    file_path = 'token/eri_sheet.json'
    
    worksheet = 'DATA'
    sheetname = 'Account'
    sh = MySheet(file_path, worksheet, sheetname)

    import pdb;pdb.set_trace()
    #df_dict = [dd for k, y in df_dict_list.items() if dd['cnm'] == cnm]
    #tm.del_id(site)
    #print(tem_ple)


