# -*- coding: utf-8 -*-
# @Time    : 2019/5/27 13:30
# @Author  : binger
import pandas as pd
import numpy as np
from orm import BaseModel
import sql


class New1(BaseModel):
    def load_sql(self):
        return sql.NEW_ORDER1

    def make_value_table(self, df):
        df["rate"] = df["成单数量"] / df["进线数量"]
        df["list_value"] = 12000 * df["rate"]
        df = df.rename(columns={"账户": "second_status", "省份": "third_status"})
        df = df[['second_status', "third_status", 'rate', 'list_value']]
        df['first_status'] = '新名单'
        return df


class New2(BaseModel):
    def load_sql(self):
        return sql.NEW_ORDER2

    def make_value_table(self, df):
        df["rate"] = df["成单数量"] / df["进线数量"]
        df["list_value"] = 12000 * df["rate"]
        df = df.rename(columns={"账户": "second_status"})
        df = df[['second_status', 'rate', 'list_value']]
        df['first_status'] = '新名单'
        return df


class NoConnected(BaseModel):
    def load_sql(self):
        return sql.NOT_CONNECTED

    def reminder(self, class_=None):
        return "该名单已到下次沟通时间，请及时拨打名单哦！"

    def make_value_table(self, df):
        cut_option = [-1, 1, 3, 6, 10, np.inf]
        cut_label_option = ["[0,1]", '(1,3]', '(3,6]', '(6,10]', '(10,+∞)']

        status_list = [
            ("进线24小时内呼叫次数", "进线后24小时内是否接通"),
            ("进线48小时内呼叫次数", "进线后48小时内是否接通"),
            ("进线72小时内呼叫次数", "进线后72小时内是否接通"),
            ("进线168小时内呼叫次数", "进线后168小时内是否接通"),
            ("进线至今呼叫总次数", "进线至今是否接通")
        ]
        third = ['(0,24]', "(24,48]", "(48,72]", "(72,168]", "(168,+∞)"]

        def group_cut_count(resp):
            index = resp[0]
            key = resp[1][0]
            is_connect = resp[1][1]

            _df = self.count_success(
                df=df[df[is_connect] == "否"],
                key=key,
                bins=cut_option,
                labels=cut_label_option,
                callback=self.reminder
            )
            _df["second_status"] = _df["third_status"]
            _df["third_status"] = third[index]
            return _df

        df_list = list(map(group_cut_count, enumerate(status_list)))
        _df = pd.concat(df_list)
        _df.reset_index(inplace=True, drop=True)
        _df["first_status"] = "未接通"
        _df["next_time_interval"] = 14
        return _df


class Connected(BaseModel):
    def load_sql(self):
        return sql.CONNECTED

    def reminder(self, class_):
        if class_ in ["较高意向", "高意向"]:
            reminder = "该名单当前意向较高，请把握机会跟进哦！"
        else:
            reminder = "该名单已到下次沟通时间，请及时拨打名单哦！"
        return reminder

    def make_value_table(self, df):
        cut_option = [-1, 1, 2, 5, 10, np.inf]
        cut_label_option = ['低意向', "较低意向", "中意向", "较高意向", "高意向"]

        status_list = [
            "接通后48小时内最大通时",
            "接通后96小时内最大通时",
            "接通后168小时内最大通时",
            "接通后336小时内最大通时",
            "接通至今最大通时"
        ]
        second = ["(0,48]", '(48,96]', '(96,168]', '(168,336]', '(336,+∞)']

        def group_cut_count(resp):
            index = resp[0]
            key = resp[1]

            _df = self.count_success(
                df=df,
                key=key,
                bins=cut_option,
                labels=cut_label_option,
                callback=self.reminder
            )
            _df["second_status"] = second[index]
            return _df

        df_list = list(map(group_cut_count, enumerate(status_list)))
        _df = pd.concat(df_list)
        _df.reset_index(inplace=True, drop=True)
        _df["first_status"] = "已接通"
        _df["next_time_interval"] = 30
        return _df


class Reserved(BaseModel):
    def load_sql(self):
        return sql.RESERVED

    def reminder(self, class_):
        if class_ in ["已排试听课待反馈", "待排体验课"]:
            return "该名单已提交预约试听，请及时联系安排体验课哦！"
        elif class_ in ["待上体验课", "待上试听课"]:
            return "该名单即将上体验课，可提醒学生按时上课哦！"
        elif class_ == "体验课跳票":
            return "该名单体验课已跳票，请及时联系并重新排体验课哦！"
        elif class_ == "试听课跳票":
            return "该名单已跳票，请及时询问原因并尝试再次提交试听哦！"
        else:
            return ''

    def make_value_table(self, df):
        # "已排试听课待反馈"
        cut_option = [-1, 3, 12, 24, 48, 72, 168, np.inf]
        cut_label_option = ['(0,1]', '(1,3]', '(3,12]', '(12,24]', '(24,48]', '(48,72]', '[48,+∞)']
        df1 = self.count_success(
            df=df[df["是否已提交设班单"] == "已提交"],
            key="设班单后首呼间隔",
            bins=cut_option,
            labels=cut_label_option,
            callback=self.reminder
        )
        df1["second_status"] = "已排试听课待反馈"

        # "待排体验课" "设班单体验课排课间隔"
        df2 = self.count_success(
            df=df[(df["是否已提交设班单"] == "已提交") & (df["是否有体验课排课记录"] == "排课")],
            key="设班单体验课排课间隔",
            bins=cut_option,
            labels=cut_label_option,
            callback=self.reminder
        )
        df2["second_status"] = "待排体验课"

        # '待上体验课' '体验课排课上课间隔'
        df3 = self.count_success(
            df=df[(df["是否已提交设班单"] == "已提交") & (df["是否有体验课排课记录"] == "排课")],
            key="体验课排课上课间隔",
            bins=cut_option,
            labels=cut_label_option,
            callback=self.reminder
        )
        df3["second_status"] = "待上体验课"

        # "待上试听课" '体验课下课试听课上课间隔'
        df4 = self.count_success(
            df=df[(df["是否已提交设班单"] == "已提交") & (df["是否有试听课排课记录"] == "排课")],
            key="体验课下课试听课上课间隔",
            bins=cut_option,
            labels=cut_label_option,
            callback=self.reminder
        )
        df4["second_status"] = "待上试听课"

        # "体验课跳票" '跳票体验课后首呼间隔'
        df5 = self.count_success(
            df=df[df["是否有体验课跳票记录"] == "跳票"],
            key="跳票体验课后首呼间隔",
            bins=cut_option,
            labels=cut_label_option,
            callback=self.reminder
        )
        df5["second_status"] = "体验课跳票"

        # "试听课跳票" '跳票试听课后首呼间隔'
        df6 = self.count_success(
            df=df[df["是否有试听课跳票记录"] == "跳票"],
            key="跳票试听课后首呼间隔",
            bins=cut_option,
            labels=cut_label_option,
            callback=self.reminder
        )
        df6["second_status"] = "试听课跳票"
        _df = pd.concat([df1, df2, df3, df4, df5, df6])
        _df.reset_index(inplace=True, drop=True)
        _df["first_status"] = "已预约"
        return _df


class WaitingOrder(BaseModel):
    def load_sql(self):
        return sql.WAITING_ORDER

    def reminder(self, class_):
        if class_ == "试听课后":
            return "名单试听后越早跟进的成单率越高，请认真对待关单电话哦！"
        else:
            return "学生提交的作业已被老师批改啦，可提醒学生及时登陆授课端查看哦！"

    def make_value_table(self, df):
        cut_option1 = [-1, 1, 12, np.inf]
        cut_label_option1 = ['(0,1]', '(1,12]', '(12,+∞)']

        cut_option2 = [-1, 1, 3, 24, 48, np.inf]
        cut_label_option2 = ['(0,1]', '(1,3]', '(3,24]', '(24,48]', '(48,+∞)']

        _df = df[df["是否实际消课"] == "消课"]
        df1 = self.count_success(
            df=_df,
            key='下课到首呼间隔(分钟)',
            bins=cut_option1,
            labels=cut_label_option1,
            callback=self.reminder
        )
        df1["second_status"] = "试听课后"

        _df = df[(df["是否未成单但已批改作业"] == "批改作业未成单") | (df["是否在成单前批改作业"] == "是")]
        df2 = self.count_success(
            df=_df,
            key='批改作业到首呼间隔(分钟)',
            bins=cut_option2,
            labels=cut_label_option2,
            callback=self.reminder
        )
        df2["second_status"] = "批改作业后"

        _df = pd.concat((df1, df2))
        _df.reset_index(inplace=True, drop=True)
        _df["first_status"] = "待成单"
        return _df


if __name__ == "__main__":
    from orm import ResultTable
    import time
    start_time = time.time()
    df = ResultTable().result()
    print(df)
    print(time.time() - start_time)
