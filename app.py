import pandas as pd # type: ignore
import numpy as np # type: ignore
from datetime import datetime
from io import BytesIO
import os

def process_file(file_path):
    """处理文件，支持csv和excel格式"""
    try:
        if not os.path.exists(file_path):
            print(f"错误: 文件 {file_path} 不存在")
            return None
            
        file_type = file_path.split('.')[-1].lower()
        if file_type == 'csv':
            return pd.read_csv(file_path, encoding='utf-8')
        elif file_type in ['xlsx', 'xls']:
            return pd.read_excel(file_path)
        else:
            print("不支持的文件格式，请使用CSV或Excel文件")
            return None
    except Exception as e:
        print(f"读取文件时发生错误: {str(e)}")
        return None

def precise_row_comparison(df_data, df_lookup, data_columns, lookup_columns):
    """
    精确逐行比对：查找表的每一行去数据表中寻找完全匹配的行
    """
    try:
        print("开始精确匹配...")
        
        # 数据预处理：处理NaN值和数据类型
        df_data_clean = df_data[data_columns].copy()
        df_lookup_clean = df_lookup[lookup_columns].copy()
        
        # 将NaN值替换为统一的标识符
        df_data_clean = df_data_clean.fillna('__NULL__')
        df_lookup_clean = df_lookup_clean.fillna('__NULL__')
        
        # 转换为字符串以确保精确匹配
        for col in data_columns:
            df_data_clean[col] = df_data_clean[col].astype(str).str.strip()
        for col in lookup_columns:
            df_lookup_clean[col] = df_lookup_clean[col].astype(str).str.strip()
        
        # 为结果添加新列
        df_lookup_result = df_lookup.copy()
        df_data_result = df_data.copy()
        
        # 初始化匹配结果列
        df_lookup_result['匹配状态'] = '未匹配'
        df_lookup_result['匹配行号'] = ''
        df_lookup_result['匹配详情'] = ''
        
        df_data_result['被匹配状态'] = '未被匹配'
        df_data_result['被匹配次数'] = 0
        
        # 统计信息
        match_stats = {
            '查找表总行数': len(df_lookup),
            '数据表总行数': len(df_data),
            '匹配成功行数': 0,
            '未匹配行数': 0,
            '重复匹配行数': 0
        }
        
        matched_data_rows = set()  # 记录数据表中已匹配的行
        
        # 逐行进行精确匹配
        for lookup_idx in range(len(df_lookup_clean)):
            lookup_row = df_lookup_clean.iloc[lookup_idx]
            
            # 显示进度
            if (lookup_idx + 1) % 100 == 0:
                print(f"已处理: {lookup_idx + 1}/{len(df_lookup_clean)} 行")
            
            # 在数据表中查找匹配行
            match_found = False
            matched_rows = []
            
            for data_idx in range(len(df_data_clean)):
                data_row = df_data_clean.iloc[data_idx]
                
                # 逐列比较
                all_columns_match = True
                match_details = []
                
                for lookup_col, data_col in zip(lookup_columns, data_columns):
                    lookup_val = lookup_row[lookup_col]
                    data_val = data_row[data_col]
                    
                    if lookup_val == data_val:
                        match_details.append(f"{lookup_col}={lookup_val}✓")
                    else:
                        match_details.append(f"{lookup_col}={lookup_val}≠{data_val}✗")
                        all_columns_match = False
                
                # 如果所有列都匹配
                if all_columns_match:
                    match_found = True
                    matched_rows.append(data_idx)
                    matched_data_rows.add(data_idx)
                    
                    # 更新数据表匹配状态
                    df_data_result.loc[data_idx, '被匹配状态'] = '已被匹配'
                    df_data_result.loc[data_idx, '被匹配次数'] += 1
            
            # 更新查找表匹配结果
            if match_found:
                if len(matched_rows) == 1:
                    df_lookup_result.loc[lookup_idx, '匹配状态'] = '匹配成功'
                    df_lookup_result.loc[lookup_idx, '匹配行号'] = f"第{matched_rows[0]+1}行"
                    match_stats['匹配成功行数'] += 1
                else:
                    df_lookup_result.loc[lookup_idx, '匹配状态'] = '重复匹配'
                    df_lookup_result.loc[lookup_idx, '匹配行号'] = f"第{','.join([str(r+1) for r in matched_rows])}行"
                    match_stats['重复匹配行数'] += 1
                
                # 记录匹配详情
                sample_match_details = []
                for lookup_col, data_col in zip(lookup_columns, data_columns):
                    val = lookup_row[lookup_col]
                    if val != '__NULL__':
                        sample_match_details.append(f"{lookup_col}={val}")
                df_lookup_result.loc[lookup_idx, '匹配详情'] = '; '.join(sample_match_details)
            else:
                df_lookup_result.loc[lookup_idx, '匹配状态'] = '未匹配'
                match_stats['未匹配行数'] += 1
                # 显示查找条件
                search_details = []
                for lookup_col in lookup_columns:
                    val = lookup_row[lookup_col]
                    if val != '__NULL__':
                        search_details.append(f"{lookup_col}={val}")
                df_lookup_result.loc[lookup_idx, '匹配详情'] = f"查找条件: {'; '.join(search_details)}"
        
        # 统计数据表中未被匹配的行数
        unmatched_data_count = len(df_data) - len(matched_data_rows)
        match_stats['数据表未被匹配行数'] = unmatched_data_count
        
        return df_data_result, df_lookup_result, match_stats
        
    except Exception as e:
        print(f"比对数据时发生错误: {str(e)}")
        return None, None, None

def create_styled_excel(df_data_result, df_lookup_result, output_path):
    """创建带样式的Excel文件"""
    try:
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            # 写入数据
            df_data_result.to_excel(writer, sheet_name='数据表结果', index=False)
            df_lookup_result.to_excel(writer, sheet_name='查找表结果', index=False)
            
            # 获取工作簿和工作表对象
            workbook = writer.book
            worksheet_data = writer.sheets['数据表结果']
            worksheet_lookup = writer.sheets['查找表结果']
            
            # 定义格式
            red_format = workbook.add_format({'bg_color': '#FFE6E6', 'font_color': '#CC0000'})
            green_format = workbook.add_format({'bg_color': '#E6F7E6', 'font_color': '#006600'})
            yellow_format = workbook.add_format({'bg_color': '#FFF2CC', 'font_color': '#CC6600'})
            
            # 设置列宽
            for worksheet in [worksheet_data, worksheet_lookup]:
                worksheet.set_column(0, 50, 12)
            
            # 为查找表结果添加条件格式
            for row in range(1, len(df_lookup_result) + 1):
                status_value = df_lookup_result.iloc[row-1]['匹配状态']
                
                if status_value == '匹配成功':
                    worksheet_lookup.set_row(row, None, green_format)
                elif status_value == '重复匹配':
                    worksheet_lookup.set_row(row, None, yellow_format)
                elif status_value == '未匹配':
                    worksheet_lookup.set_row(row, None, red_format)
            
            # 为数据表结果添加条件格式
            for row in range(1, len(df_data_result) + 1):
                status_value = df_data_result.iloc[row-1]['被匹配状态']
                
                if status_value == '已被匹配':
                    worksheet_data.set_row(row, None, green_format)
                else:
                    worksheet_data.set_row(row, None, red_format)
        
        print(f"Excel文件已保存到: {output_path}")
        return True
        
    except Exception as e:
        print(f"保存Excel文件时发生错误: {str(e)}")
        return False

def print_stats(stats):
    """打印统计信息"""
    print("\n" + "="*50)
    print("📈 比对结果统计")
    print("="*50)
    for key, value in stats.items():
        if '总行数' in key:
            print(f"{key}: {value}")
        else:
            percentage = value / stats['查找表总行数'] * 100 if stats['查找表总行数'] > 0 else 0
            print(f"{key}: {value} ({percentage:.1f}%)")
    print("="*50)

def main():
    print("🔍 精确数据比对工具")
    print("="*50)
    
    # 获取文件路径
    print("\n请输入文件路径：")
    data_file = input("数据表文件路径: ").strip().replace('"', '')
    lookup_file = input("查找表文件路径: ").strip().replace('"', '')
    
    # 读取文件
    print("\n正在读取文件...")
    df_data = process_file(data_file)
    df_lookup = process_file(lookup_file)
    
    if df_data is None or df_lookup is None:
        print("❌ 文件读取失败，程序退出")
        return
    
    # 显示数据预览
    print(f"\n📊 数据表预览 (共{len(df_data)}行):")
    print(df_data.head())
    print(f"\n🔎 查找表预览 (共{len(df_lookup)}行):")
    print(df_lookup.head())
    
    # 显示列名
    print(f"\n数据表列名: {list(df_data.columns)}")
    print(f"查找表列名: {list(df_lookup.columns)}")
    
    # 选择比较列
    print("\n请选择要比较的列（用逗号分隔，例如: 0,1,2 或 姓名,身份证号）:")
    data_cols_input = input("数据表的列: ").strip()
    lookup_cols_input = input("查找表的列: ").strip()
    
    try:
        # 尝试按索引解析
        if data_cols_input.replace(',', '').replace(' ', '').isdigit():
            data_columns = [df_data.columns[int(i.strip())] for i in data_cols_input.split(',')]
        else:
            data_columns = [col.strip() for col in data_cols_input.split(',')]
            
        if lookup_cols_input.replace(',', '').replace(' ', '').isdigit():
            lookup_columns = [df_lookup.columns[int(i.strip())] for i in lookup_cols_input.split(',')]
        else:
            lookup_columns = [col.strip() for col in lookup_cols_input.split(',')]
            
    except Exception as e:
        print(f"❌ 列选择错误: {str(e)}")
        return
    
    if len(data_columns) != len(lookup_columns):
        print("❌ 两个表格选择的列数必须相同！")
        return
    
    print(f"\n将比较以下列:")
    for d_col, l_col in zip(data_columns, lookup_columns):
        print(f"  数据表[{d_col}] ↔ 查找表[{l_col}]")
    
    # 开始比对
    print("\n🚀 开始精确比对...")
    result_data, result_lookup, stats = precise_row_comparison(
        df_data, df_lookup, data_columns, lookup_columns
    )
    
    if result_data is not None:
        print("\n✅ 比对完成！")
        
        # 显示统计结果
        print_stats(stats)
        
        # 保存结果
        output_path = f"精确比对结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        if create_styled_excel(result_data, result_lookup, output_path):
            print(f"\n📥 结果已保存到: {output_path}")
        
        # 显示部分结果预览
        print(f"\n🔍 查找表结果预览:")
        preview_cols = ['匹配状态', '匹配行号', '匹配详情']
        if len(result_lookup.columns) > 3:
            preview_cols.extend(list(result_lookup.columns)[:3])
        print(result_lookup[preview_cols].head(10))
    
    print("\n程序执行完毕！")

if __name__ == "__main__":
    main()
