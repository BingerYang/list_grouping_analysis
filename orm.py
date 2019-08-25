# -*- coding: utf-8 -*-
# @Time    : 2019/5/16 15:15
# @Author  : binger
import pandas as pd
import pandas.io.sql
# import weakref
import sqlpools

#
# def make_value(self):
#
#     # weakref.proxy(self)
#     return obj.conversion_rate(), obj.value()
instance_list = []


class ModeMetaClass(type):
    instance_list = []

    def __new__(cls, name, bases, attrs):
        if name == "BaseModel":
            return type.__new__(cls, name, bases, attrs)

        # 继承 ValueBaseModel 的类
        mode = type.__new__(cls, name, bases, attrs)
        instance_list.append(mode)
        return mode


class BaseModel(metaclass=ModeMetaClass):

    def load_sql(self):
        print("here????")
        """
        需要重载
        :return: 返回 Sql
        """
        pass

    def make_value_table(self, df):
        """
        需要重载
        :param df:
        :return: 返回处理好的 df
        """
        pass

    def reminder(self, class_):
        """
        根据 class_ 输出不同提示语
        :param class_:
        :return:
        """
        pass

    def load_df_by_sql(self):
        sql = self.load_sql()
        with sqlpools.get_conn("reportChart", r=True) as conn:
            try:
                df = pd.read_sql(sql, con=conn)
            except pandas.io.sql.DatabaseError:
                import traceback
                print(traceback.format_exc())
        return df

    def count_success(self, df, key, bins, labels, to_pandas=True, callback=None):
        cut = pd.cut(df[key], bins=bins, labels=labels)
        cut_rate_list = []
        for class_, group in df.groupby(cut):
            reminder = callback(class_) if class_ else ""
            try:
                rate = group[group["是否成单"] == "是"].shape[0] / group.shape[0]
            except ZeroDivisionError as e:
                rate = 0
            cut_rate_list.append(
                {"third_status": class_, "rate": rate, "list_value": 12000 * rate, "reminder": reminder})
        if to_pandas:
            return pd.DataFrame(cut_rate_list)
        else:
            return cut_rate_list


class ResultTable(object):
    _result = None

    def run(self, cls):
        obj = cls()
        df = obj.load_df_by_sql()
        return obj.make_value_table(df)

    def result(self):
        df = pd.concat(list(map(lambda cls: self.run(cls), instance_list)))
        df.reset_index(drop=True, inplace=True)
        return df

    def save_to_sql(self):
        df = self.result()
        with sqlpools.get_conn(module="reportChart", r=True) as conn:
            df.to_sql(
                name="sale_list_dict",
                con=conn,
                if_exists="append",
                index=False
            )
