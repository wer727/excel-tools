import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from io import BytesIO
import time

# 页面配置
st.set_page_config(
    page_title="精确数据比对工具", 
    page_icon="🔍", 
    layout="wide",
    initial_sidebar_state="expanded"
)

def process_file(uploaded_file):
    """处理上传的文件，支持csv和excel格式"""
    try:
        file_type = uploaded_file.name.split('.')[-1].lower()
        if file_type == 'csv':
            return pd.read_csv(uploaded_file, encoding='utf-8')
        elif file_type in ['xlsx', 'xls']:
            return pd.read_excel(uploaded_file)
        else:
            st.error("❌ 不支持的文件格式，请上传CSV或Excel文件")
            return None
    except UnicodeDecodeError:
        try:
            # 尝试其他编码
            return pd.read_csv(uploaded_file, encoding='gbk')
        except:
            st.error("❌ 文件编码错误，请检查文件格式")
            return None
    except Exception as e:
        st.error(f"❌ 读取文件时发生错误: {str(e)}")
        return None

def precise_row_comparison(df_data, df_lookup, data_columns, lookup_columns):
    """
    精确逐行比对：查找表的每一行去数据表中寻找完全匹配的行
    """
    try:
        # 创建进度条
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        status_text.text("正在预处理数据...")
        
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
        total_rows = len(df_lookup_clean)
        for lookup_idx in range(total_rows):
            lookup_row = df_lookup_clean.iloc[lookup_idx]
            
            # 更新进度
            progress = (lookup_idx + 1) / total_rows
            progress_bar.progress(progress)
            status_text.text(f"正在匹配第 {lookup_idx + 1}/{total_rows} 行...")
            
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
        
        # 清除进度条
        progress_bar.empty()
        status_text.empty()
        
        return df_data_result, df_lookup_result, match_stats
        
    except Exception as e:
        st.error(f"❌ 比对数据时发生错误: {str(e)}")
        return None, None, None

def create_styled_excel(df_data_result, df_lookup_result, data_columns, lookup_columns):
    """创建带样式的Excel文件"""
    output = BytesIO()
    
    try:
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
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
            header_format = workbook.add_format({'bold': True, 'bg_color': '#D9EAD3'})
            
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
        
        output.seek(0)
        return output.getvalue()
        
    except Exception as e:
        st.error(f"❌ 创建Excel文件时发生错误: {str(e)}")
        return None

def display_stats_cards(stats):
    """显示统计卡片"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="📊 查找表总行数", 
            value=stats['查找表总行数']
        )
    
    with col2:
        success_rate = stats['匹配成功行数'] / stats['查找表总行数'] * 100 if stats['查找表总行数'] > 0 else 0
        st.metric(
            label="✅ 匹配成功", 
            value=stats['匹配成功行数'],
            delta=f"{success_rate:.1f}%"
        )
    
    with col3:
        fail_rate = stats['未匹配行数'] / stats['查找表总行数'] * 100 if stats['查找表总行数'] > 0 else 0
        st.metric(
            label="❌ 未匹配", 
            value=stats['未匹配行数'],
            delta=f"{fail_rate:.1f}%"
        )
    
    with col4:
        st.metric(
            label="🔄 重复匹配", 
            value=stats['重复匹配行数']
        )

def main():
    # 初始化session state
    if 'comparison_results' not in st.session_state:
        st.session_state.comparison_results = None
    if 'comparison_stats' not in st.session_state:
        st.session_state.comparison_stats = None
    if 'excel_data' not in st.session_state:
        st.session_state.excel_data = None
    if 'result_timestamp' not in st.session_state:
        st.session_state.result_timestamp = None
    if 'show_comparison_section' not in st.session_state:
        st.session_state.show_comparison_section = True
    
    # 页面标题
    st.title("🔍 精确数据比对工具")
    st.markdown("---")
    
    # 侧边栏说明和控制
    with st.sidebar:
        st.header("📖 使用说明")
        st.markdown("""
        ### 功能特性
        - 🎯 **精确逐行匹配**：查找表的每一行数据在数据表中寻找完全匹配的行
        - 🔗 **多列组合匹配**：支持选择多个列进行组合匹配
        - 📊 **详细匹配报告**：显示匹配状态、匹配行号和详细信息
        - 📁 **Excel导出**：带颜色标识的结果文件
        
        ### 使用步骤
        1. 上传数据表（被查找的表格）
        2. 上传查找表（包含查找条件的表格）
        3. 选择对应的比较列
        4. 点击开始比对
        5. 查看结果并下载报告
        
        ### 文件要求
        - 支持格式：CSV、Excel (.xlsx/.xls)
        - 最大文件大小：200MB
        - 编码支持：UTF-8、GBK
        """)
        
        # 控制按钮
        st.markdown("---")
        st.header("🎛️ 操作控制")
        
        if st.button("🔄 重新开始", use_container_width=True):
            # 清除所有session state
            for key in ['comparison_results', 'comparison_stats', 'excel_data', 'result_timestamp']:
                if key in st.session_state:
                    del st.session_state[key]
            st.session_state.show_comparison_section = True
            st.rerun()
        
        # 显示结果状态
        if st.session_state.comparison_results is not None:
            st.success("✅ 有可用的比对结果")
            if st.session_state.result_timestamp:
                st.info(f"⏰ 生成时间: {st.session_state.result_timestamp}")
            
            if st.button("📋 查看结果详情", use_container_width=True):
                st.session_state.show_comparison_section = False
                st.rerun()
            
            if st.button("📁 跳转到下载", use_container_width=True):
                st.session_state.show_comparison_section = False
                # 滚动到页面底部的下载按钮
                st.markdown('<script>window.scrollTo(0, document.body.scrollHeight);</script>', unsafe_allow_html=True)
                st.rerun()
    
    # 如果有结果且不显示比对区域，直接跳到结果展示
    if st.session_state.comparison_results is not None and not st.session_state.show_comparison_section:
        show_results_section()
        return
    
    # 文件上传区域
    st.subheader("📁 文件上传")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### 📋 数据表（被查找表）")
        uploaded_file_data = st.file_uploader(
            "选择数据表文件", 
            type=['csv', 'xlsx', 'xls'],
            help="这是被查找的主数据表",
            key="data_file"
        )
    
    with col2:
        st.markdown("##### 🔎 查找表（查找条件表）")
        uploaded_file_lookup = st.file_uploader(
            "选择查找表文件", 
            type=['csv', 'xlsx', 'xls'],
            help="这是包含查找条件的表格",
            key="lookup_file"
        )

    if uploaded_file_data and uploaded_file_lookup:
        # 读取文件
        with st.spinner("正在读取文件..."):
            df_data = process_file(uploaded_file_data)
            df_lookup = process_file(uploaded_file_lookup)

        if df_data is not None and df_lookup is not None:
            # 显示数据预览
            st.markdown("---")
            st.subheader("📊 数据预览")
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**数据表预览** (共{len(df_data):,}行)")
                st.dataframe(df_data.head(), use_container_width=True)
                
            with col2:
                st.markdown(f"**查找表预览** (共{len(df_lookup):,}行)")
                st.dataframe(df_lookup.head(), use_container_width=True)

            # 列选择区域
            st.markdown("---")
            st.subheader("🎯 选择比较列")
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("##### 数据表的列")
                data_columns = st.multiselect(
                    "选择数据表中用于匹配的列", 
                    df_data.columns,
                    help="选择数据表中用于匹配的列，可以选择多个列进行组合匹配"
                )
                
            with col2:
                st.markdown("##### 查找表的列")
                lookup_columns = st.multiselect(
                    "选择查找表的查找条件列", 
                    df_lookup.columns,
                    help="选择查找表中的查找条件列，必须与数据表选择的列数量相同"
                )

            # 显示匹配关系
            if data_columns and lookup_columns:
                st.info("🔗 **列匹配关系预览：**")
                if len(data_columns) == len(lookup_columns):
                    match_info = []
                    for i, (d_col, l_col) in enumerate(zip(data_columns, lookup_columns), 1):
                        match_info.append(f"{i}. 数据表[{d_col}] ↔ 查找表[{l_col}]")
                    st.success("\n".join(match_info))
                else:
                    st.error(f"❌ 列数量不匹配！数据表选择了{len(data_columns)}列，查找表选择了{len(lookup_columns)}列")

            # 比对按钮和结果
            st.markdown("---")
            if st.button("🚀 开始精确比对", type="primary", use_container_width=True):
                if len(data_columns) != len(lookup_columns):
                    st.error("❌ 两个表格选择的列数必须相同！")
                elif not data_columns or not lookup_columns:
                    st.error("❌ 请至少选择一列进行比较！")
                else:
                    # 执行比对
                    start_time = time.time()
                    
                    result_data, result_lookup, stats = precise_row_comparison(
                        df_data, df_lookup, data_columns, lookup_columns
                    )
                    
                    if result_data is not None:
                        processing_time = time.time() - start_time
                        
                        # 保存结果到session state
                        st.session_state.comparison_results = {
                            'result_data': result_data,
                            'result_lookup': result_lookup,
                            'df_data': df_data,
                            'df_lookup': df_lookup,
                            'data_columns': data_columns,
                            'lookup_columns': lookup_columns
                        }
                        st.session_state.comparison_stats = stats
                        st.session_state.result_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        
                        # 生成Excel并保存到session state
                        with st.spinner("正在生成Excel文件..."):
                            excel_data = create_styled_excel(result_data, result_lookup, data_columns, lookup_columns)
                            st.session_state.excel_data = excel_data
                        
                        st.success(f"✅ 比对完成！处理时间: {processing_time:.2f}秒")
                        st.info("💡 结果已保存，您可以随时查看和下载，页面刷新不会丢失！")
                        
                        # 自动跳转到结果展示
                        st.session_state.show_comparison_section = False
                        time.sleep(1)  # 给用户一点时间看到成功消息
                        st.rerun()

    # 如果有保存的结果，显示结果区域
    if st.session_state.comparison_results is not None:
        show_results_section()

def show_results_section():
    """显示结果区域"""
    if st.session_state.comparison_results is None:
        return
    
    results = st.session_state.comparison_results
    stats = st.session_state.comparison_stats
    
    result_data = results['result_data']
    result_lookup = results['result_lookup']
    df_data = results['df_data']
    df_lookup = results['df_lookup']
    
    st.markdown("---")
    st.header("📈 比对结果")
    
    if st.session_state.result_timestamp:
        st.caption(f"⏰ 生成时间: {st.session_state.result_timestamp}")
    
    # 显示统计结果
    st.subheader("📊 统计概览")
    display_stats_cards(stats)
    
    # 详细统计表
    with st.expander("📋 查看详细统计", expanded=False):
        stats_df = pd.DataFrame([
            {"项目": k, "数量": v, "占比": f"{v/stats['查找表总行数']*100:.2f}%" if '行数' in k and stats['查找表总行数'] > 0 else "-"}
            for k, v in stats.items()
        ])
        st.dataframe(stats_df, use_container_width=True)
    
    # 显示结果预览
    st.subheader("🔍 结果预览")
    
    tab1, tab2 = st.tabs(["📋 查找表结果", "📊 数据表结果"])
    
    with tab1:
        st.markdown("显示查找表的匹配结果（前100行）")
        display_cols = ['匹配状态', '匹配行号', '匹配详情'] + list(df_lookup.columns)
        st.dataframe(
            result_lookup[display_cols].head(100), 
            use_container_width=True
        )
    
    with tab2:
        st.markdown("显示数据表的被匹配状态（前100行）")
        display_cols = list(df_data.columns) + ['被匹配状态', '被匹配次数']
        st.dataframe(
            result_data[display_cols].head(100), 
            use_container_width=True
        )
    
    # 下载区域
    st.markdown("---")
    st.subheader("📥 下载完整结果")
    
    # 创建两列布局
    col1, col2 = st.columns([2, 1])
    
    with col1:
        if st.session_state.excel_data:
            st.download_button(
                label="📥 下载详细比对结果 (Excel)",
                data=st.session_state.excel_data,
                file_name=f"精确比对结果_{st.session_state.result_timestamp.replace(':', '-').replace(' ', '_')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                use_container_width=True
            )
        else:
            if st.button("🔄 重新生成Excel文件", use_container_width=True):
                with st.spinner("正在生成Excel文件..."):
                    excel_data = create_styled_excel(
                        result_data, result_lookup, 
                        results['data_columns'], results['lookup_columns']
                    )
                    st.session_state.excel_data = excel_data
                st.rerun()
    
    with col2:
        st.info("""
        📋 **Excel文件说明：**
        - 🟢 绿色：匹配成功
        - 🟡 黄色：重复匹配  
        - 🔴 红色：未匹配
        """)
    
    # 下载状态提示
    st.success("✅ 点击下载按钮不会刷新页面，结果已保存！")
    
    # 返回比对区域的按钮
    if st.button("🔄 进行新的比对", use_container_width=True):
        st.session_state.show_comparison_section = True
        st.rerun()
    
    # 底部信息
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666; padding: 20px;'>
            <p>🔍 精确数据比对工具 | 支持大数据量处理 | 精确逐行匹配</p>
        </div>
        """, 
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
