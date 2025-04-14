import streamlit as st
import pandas as pd
import os
from datetime import datetime
from io import BytesIO

def process_excel(df, merge_columns, count_column):
    """处理Excel数据，合并重复数据并计数"""
    try:
        # 处理空值
        for col in merge_columns:
            df[col] = df[col].fillna('').astype(str)
        
        # 根据多列条件计算重复数据
        df['_temp_key'] = df[merge_columns].apply(tuple, axis=1)
        value_counts = df['_temp_key'].value_counts()
        
        # 创建新的DataFrame，去除重复项
        df_unique = df.drop_duplicates(subset=merge_columns)
        df_unique[count_column] = df_unique[merge_columns].apply(tuple, axis=1).map(value_counts)
        
        return df_unique.drop('_temp_key', axis=1)
    except Exception as e:
        st.error(f"处理数据时发生错误: {str(e)}")
        return None

def compare_excel(df_data, df_lookup, data_columns, lookup_columns):
    """比较两个DataFrame的差异"""
    try:
        # 为每对列进行匹配检查
        results_data = []
        results_lookup = []
        
        # 创建数据副本以避免修改原始数据
        df_data = df_data.copy()
        df_lookup = df_lookup.copy()
        
        for col_data, col_lookup in zip(data_columns, lookup_columns):
            # 处理空值并转换为字符串
            df_data[col_data] = df_data[col_data].fillna('').astype(str).str.strip()
            df_lookup[col_lookup] = df_lookup[col_lookup].fillna('').astype(str).str.strip()
            
            # 将数据和查找值转为集合
            data_values = set(df_data[col_data].values)
            lookup_values = set(df_lookup[col_lookup].values)
            
            # 在数据表中标记结果
            df_data[f"{col_data}_匹配结果"] = df_data[col_data].apply(
                lambda x: "匹配" if x in lookup_values else ("缺失" if x == '' else "未匹配")
            )
            
            # 在查找值表中标记结果
            df_lookup[f"{col_lookup}_匹配结果"] = df_lookup[col_lookup].apply(
                lambda x: "匹配" if x in data_values else ("缺失" if x == '' else "未匹配")
            )
            
            # 统计结果
            results_data.append(df_data[f"{col_data}_匹配结果"].value_counts())
            results_lookup.append(df_lookup[f"{col_lookup}_匹配结果"].value_counts())
        
        return df_data, df_lookup, results_data, results_lookup
    except Exception as e:
        st.error(f"比对数据时发生错误: {str(e)}")
        return None, None, None, None

def apply_excel_styles(writer, df, sheet_name, columns_to_check):
    """应用Excel样式"""
    try:
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]
        
        # 定义样式
        red_format = workbook.add_format({
            'font_color': 'red',
            'font_name': '微软雅黑',
            'align': 'left',
            'valign': 'vcenter'
        })
        
        normal_format = workbook.add_format({
            'font_name': '微软雅黑',
            'align': 'left',
            'valign': 'vcenter'
        })
        
        # 设置列宽
        for idx, col in enumerate(df.columns):
            max_length = max(
                df[col].astype(str).apply(len).max(),
                len(str(col))
            )
            worksheet.set_column(idx, idx, max_length + 2)
        
        # 获取列的索引位置并应用样式
        for col_name in columns_to_check:
            if col_name in df.columns:  # 确保列存在
                col_idx = df.columns.get_loc(col_name)
                result_col = f"{col_name}_匹配结果"
                
                if result_col in df.columns:  # 确保结果列存在
                    # 为每一行应用条件格式
                    for row_idx in range(len(df)):
                        cell_value = str(df.iloc[row_idx][col_name])
                        match_result = df.iloc[row_idx][result_col]
                        
                        if match_result == "未匹配":
                            worksheet.write(row_idx + 1, col_idx, cell_value, red_format)
                        else:
                            worksheet.write(row_idx + 1, col_idx, cell_value, normal_format)
    except Exception as e:
        st.error(f"应用Excel样式时发生错误: {str(e)}")

def main():
    st.set_page_config(page_title="Excel工具", page_icon="📊", layout="wide")
    st.title("Excel文件处理工具")
    
    # 侧边栏 - 功能选择
    function = st.sidebar.radio(
        "选择功能",
        ["合并重复数据", "表格数据匹配", "表格数据填充"]
    )
    
    if function == "合并重复数据":
        st.header("合并重复数据")
        
        # 文件上传
        uploaded_file = st.file_uploader("上传Excel文件", type=['xlsx', 'xls'])
        
        if uploaded_file:
            # 读取Excel文件
            df = pd.read_excel(uploaded_file)
            
            # 显示数据预览
            st.subheader("数据预览")
            st.dataframe(df.head())
            
            # 选择列
            columns = df.columns.tolist()
            merge_cols = st.multiselect("选择用于判断重复的列", columns)
            count_col = st.selectbox("选择计数结果写入的列", columns)
            
            if st.button("处理数据"):
                if not merge_cols:
                    st.warning("请选择用于判断重复的列")
                else:
                    result_df = process_excel(df, merge_cols, count_col)
                    if result_df is not None:
                        st.success("处理完成！")
                        
                        # 显示结果
                        st.subheader("处理结果")
                        st.dataframe(result_df)
                        
                        # 下载结果
                        output = BytesIO()
                        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                            result_df.to_excel(writer, index=False)
                        
                        st.download_button(
                            label="下载结果",
                            data=output.getvalue(),
                            file_name=f"合并结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
    
    elif function == "表格数据匹配":
        st.header("表格数据匹配")
        
        col1, col2 = st.columns(2)
        
        with col1:
            data_file = st.file_uploader("上传数据表", type=['xlsx', 'xls'])
        with col2:
            lookup_file = st.file_uploader("上传查找值表", type=['xlsx', 'xls'])
            
        if data_file and lookup_file:
            # 读取文件
            df_data = pd.read_excel(data_file)
            df_lookup = pd.read_excel(lookup_file)
            
            # 显示数据预览
            st.subheader("数据表预览")
            st.dataframe(df_data.head())
            st.subheader("查找值表预览")
            st.dataframe(df_lookup.head())
            
            # 选择要比对的列
            data_columns = st.multiselect("选择数据表要比对的列", df_data.columns)
            lookup_columns = st.multiselect("选择查找值表对应的列", df_lookup.columns)
            
            if st.button("开始比对"):
                if len(data_columns) != len(lookup_columns):
                    st.warning("两个表格选择的列数必须相同")
                else:
                    result_data, result_lookup, stats_data, stats_lookup = compare_excel(
                        df_data, df_lookup, data_columns, lookup_columns
                    )
                    
                    if result_data is not None:
                        st.success("比对完成！")
                        
                        # 显示统计结果
                        st.subheader("比对结果统计")
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write("数据表统计")
                            for col, stats in zip(data_columns, stats_data):
                                st.write(f"{col}列统计:")
                                st.write(stats)
                                
                        with col2:
                            st.write("查找值表统计")
                            for col, stats in zip(lookup_columns, stats_lookup):
                                st.write(f"{col}列统计:")
                                st.write(stats)
                        
                        # 下载结果
                        output = BytesIO()
                        try:
                            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                                # 写入数据
                                result_data.to_excel(writer, sheet_name='数据表对比结果', index=False)
                                result_lookup.to_excel(writer, sheet_name='查找值表对比结果', index=False)
                                
                                # 应用样式
                                apply_excel_styles(writer, result_data, '数据表对比结果', data_columns)
                                apply_excel_styles(writer, result_lookup, '查找值表对比结果', lookup_columns)
                            
                            st.download_button(
                                label="下载比对结果",
                                data=output.getvalue(),
                                file_name=f"对比结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
                        except Exception as e:
                            st.error(f"生成Excel文件时发生错误: {str(e)}")

    elif function == "表格数据填充":
        st.header("表格数据填充")
        st.info("此功能正在开发中...")

if __name__ == "__main__":
    main() 
