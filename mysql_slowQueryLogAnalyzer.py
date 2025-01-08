#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
解析慢日志生成报告，包含json，markdown，Excel 3种格式
"""

import re
import json
from collections import defaultdict
from datetime import datetime
import argparse
import os
import xlsxwriter


class SlowQueryLogAnalyzer:
    def __init__(self):
        self.queries = []
        self.current_db = None
        # 按数据库分组的查询统计
        self.tables = {}
        self.db_stats = defaultdict(lambda: {
            'query_count': 0,
            'queries': [],
            'tables': defaultdict(int)  # 记录每个表的查询次数
        })

    def parse_log_file(self, filename):
        """解析MySQL慢查询日志文件"""
        current_query = {}

        with open(filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        for line in lines:
            line = line.strip()

            # 跳过空行
            if not line or line.startswith('--'):
                continue

            # 解析时间戳
            if line.startswith('# Time:'):
                if current_query:
                    self._process_query(current_query)
                current_query = {'timestamp': line[7:].strip()}

            # 解析用户信息
            elif line.startswith('# User@Host:'):
                user_host = re.search(r'User@Host:\s+(\w+)\[.*\]\s+@\s+\[(.*?)\]', line)
                if user_host:
                    current_query['user'] = user_host.group(1)
                    current_query['host'] = user_host.group(2)

            # 解析查询时间和扫描行数
            elif line.startswith('# Query_time:'):
                stats = re.search(
                    r'Query_time:\s+([\d.]+)\s+Lock_time:\s+([\d.]+)\s+Rows_sent:\s+(\d+)\s+Rows_examined:\s+(\d+)',
                    line)
                if stats:
                    current_query['query_time'] = float(stats.group(1))
                    current_query['lock_time'] = float(stats.group(2))
                    current_query['rows_sent'] = int(stats.group(3))
                    current_query['rows_examined'] = int(stats.group(4))

            # 处理use语句
            elif line.lower().startswith('use '):
                self.current_db = line[4:].strip(';').strip('`')
                continue

            # 处理实际的SQL语句
            elif not line.startswith('#') and not line.lower().startswith('set timestamp'):
                if 'sql' not in current_query:
                    current_query['sql'] = line
                    current_query['database'] = self.current_db
                else:
                    current_query['sql'] += ' ' + line

        # 处理最后一个查询
        if current_query:
            self._process_query(current_query)

    def extract_tables(self, sql):
        """提取SQL中的表名"""
        patterns = [
            # 匹配 SELECT 中的表名
            re.compile(r'FROM\s+([^\s,;()]+)', re.IGNORECASE),
            re.compile(r'JOIN\s+([^\s,;()]+)', re.IGNORECASE),
            # 匹配 INSERT INTO 中的表名
            re.compile(r'INSERT\s+INTO\s+([^\s,;()]+)', re.IGNORECASE),
            # 匹配 UPDATE 中的表名
            re.compile(r'UPDATE\s+([^\s,;()]+)', re.IGNORECASE),
        ]

        table_names = set()

        # 遍历所有正则表达式模式，提取匹配的表名
        for pattern in patterns:
            tables = re.findall(pattern, sql)
            table_names.update(tables)
        if not table_names:
            print('No tables found', sql)
            # exit(1)

        return table_names

    def _process_query(self, query):
        """处理单个查询"""
        if 'sql' not in query:
            return

        # 提取涉及的表
        tables = self.extract_tables(query['sql'])
        self.generateTableInfo(tables, query)
        query['tables'] = list(tables)

        # 更新数据库统计
        db_name = query.get('database', 'unknown')
        self.db_stats[db_name]['query_count'] += 1
        self.db_stats[db_name]['queries'].append(query)

        # 更新表统计
        for table in tables:
            self.db_stats[db_name]['tables'][table] += 1

    def generateTableInfo(self, table_key, query):
        if not table_key:
            return
        table_key = str(table_key)
        if table_key not in self.tables:
            self.tables[table_key] = {
                'db': [],
            }
        db = query.get('database', 'unknown')
        if db not in self.tables[table_key]["db"]:
            self.tables[table_key]["db"].append(db)
        sql = query.get('sql', 'unknown')
        tmpMap = {
            'query_count': 0,
            'max_time': 0,
            'total_time': 0,
            'avg_time': 0,
        }

        if sql in self.tables[table_key]:
            tmpMap = self.tables[table_key][sql]

        tmpMap["query_count"] += 1
        if query.get("query_time", 0) > tmpMap["max_time"]:
            tmpMap["max_time"] = query['query_time']
        tmpMap["total_time"] += query.get("query_time", 0)
        tmpMap["avg_time"] = tmpMap["total_time"] / tmpMap["query_count"]
        self.tables[table_key][sql] = tmpMap

    def analyze(self, log_file, output_prefix='slow_query'):
        """分析日志并生成报告"""
        # 解析日志文件
        print("正在解析日志文件...")
        self.parse_log_file(log_file)

        workbook = xlsxwriter.Workbook(f"{output_prefix}_report.xlsx")
        worksheet = workbook.add_worksheet()
        # 写入表头
        headers = ['表', '次数', '平均时间', '最大时间', '处理方式', 'db', 'sql']
        for col_num, header in enumerate(headers):
            worksheet.write(0, col_num, header)


        # 生成详细报告
        print("正在生成详细报告...")
        sort_data = []
        markdown = """| 表                                                        | 次数 | 平均时间 | 最大时间 | 处理方式                      |db|sql|
|----------------------------------------------------------|----|------|------|---------------------------|-----|-------|\n"""

        row_num = 0
        for k, v in self.tables.items():
            for k1, v1 in v.items():
                if k1 != "db":
                    row_num += 1
                    tmp1 = "-------"
                    if "SQL_NO_CACHE" in k1:
                        tmp1 = "数据库备份导致的"

                    tmp_data = [k, v1["query_count"], v1["avg_time"],v1["max_time"],tmp1, v["db"], k1]
                    for col_num, value in enumerate(tmp_data):
                        if type(value) == list:
                            worksheet.write(row_num, col_num, ' '.join(value))
                        else:
                            worksheet.write(row_num, col_num, value)
                    tmp_data = "|{0}|{1}|{2:.2f}|{3:.2f}|{6}|{5}|{4}|\n".format(k, v1["query_count"], v1["avg_time"],
                                                                                v1["max_time"], k1[:200],
                                                                                str(v["db"])[:50], tmp1)
                    sort_data.append([v1["query_count"], k, tmp_data])


        workbook.close()
        print(f"Excel报告已生成: {output_prefix}_report.xlsx")

        sorted_data = sorted(sort_data, key=lambda x: (x[0], x[1]), reverse=True)
        for data in sorted_data:
            markdown += data[2]
        markdownfile = f"{output_prefix}_markdown.md"
        with open(markdownfile, 'w', encoding='utf-8') as f:
            f.write(markdown)


        report_file = f"{output_prefix}_report_table.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            self.tables["tables"] = list(self.tables.keys())
            json.dump(self.tables, f, indent=2, ensure_ascii=False)

def main():
    parser = argparse.ArgumentParser(description='MySQL慢查询日志分析工具')
    parser.add_argument('-l', '--log_file', default="completion_video_share_slow.log", help='MySQL慢查询日志文件路径')
    parser.add_argument('-o', '--output', default='slow_query',
                        help='输出文件前缀 (默认: slow_query)')

    args = parser.parse_args()

    if not os.path.exists(args.log_file):
        print(f"错误: 无法找到日志文件 '{args.log_file}'")
        return

    analyzer = SlowQueryLogAnalyzer()
    analyzer.analyze(args.log_file, args.output)


if __name__ == '__main__':
    main()
