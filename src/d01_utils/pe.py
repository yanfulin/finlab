from finlab import data
from finlab.backtest import sim
import finlab
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.figure_factory as ff
from pathlib import Path
import os
from dotenv import load_dotenv

def configure():
    dotenv_path = Path(Path.home(),'PycharmProjects/finlab/conf/.env')
    # print(Path.cwd(),Path.home())
    # print(dotenv_path)
    # print(os.path.expanduser("~/PycharmProjects"))
    load_dotenv(dotenv_path)
    # finlab_api=os.getenv("finlab_key")

def get_pe_return():
    # 下載本益比資料
    configure()
    finlab.login(os.getenv("finlab_key"))
    pe = data.get('price_earning_ratio:本益比')

    # 設定測試區間(最後測試 PE = 50 ~ 1000)
    test_range = list(np.arange(0,55,5)) + [1000]

    # 回測序列資料集
    return_dataset = {}

    for start, end in zip(test_range, test_range[1:]):

        # 選股條件
        position = (pe > start) & (pe < end)

        # 進行回測，upload設為false，不上傳資料到平台，加快回測
        report = sim(position=position, resample='M', upload=False)

        # 製作繪圖dataframe
        return_dataset[f"{start}-{end}"] = report.creturn

    return_dataset = pd.DataFrame(return_dataset)
    return return_dataset

return_dataset = get_pe_return()
fig = px.line(return_dataset, title='pe multiple backtest')
fig.show()

fig = px.bar(return_dataset.iloc[-1], color=return_dataset.iloc[-1].values, title='pe multiple backtest bar')
fig.show()
pe = data.get('price_earning_ratio:本益比')
position= (pe>10) & (pe<15)
report=sim(position=position,resample='M',name="策略教學範例:pe_single_factor",upload=True)
report.benchmark = data.get('benchmark_return:發行量加權股價報酬指數').squeeze()
report.display()
report.creturn

category = ['光電業', '其他', '其他電子業', '化學工業', '半導體', '塑膠工業', '建材營造', '文化創意業', '橡膠工業', '水泥工業',
            '汽車工業', '油電燃氣業', '玻璃陶瓷', '生技醫療', '紡織纖維', '航運業', '觀光事業', '貿易百貨',
            '資訊服務業', '農業科技', '通信網路業', '造紙工業', '金融', '鋼鐵工業', '電器電纜', '電子商務',
            '電子通路業', '電子零組件', '電機機械', '電腦及週邊', '食品工業']

final_return_dataset = {}
for cate in category:

    data.set_universe(category=cate)

    try:
        return_dataset = get_pe_return()
        final_return_dataset[cate] = return_dataset
    except:
        pass

heatmap = {}
for key, value in final_return_dataset.items():
    heatmap[key] = value.iloc[-1]
df_heatmap = pd.DataFrame(heatmap)

fig = px.imshow(np.log(df_heatmap))
fig.show()

df_heatmap=round(df_heatmap,2)
x=list(df_heatmap.columns)
y=list(df_heatmap.index)
z=df_heatmap.values

fig = ff.create_annotated_heatmap(z=z, x=x, y=y, annotation_text=z, showscale=True, reversescale=True, font_colors=['#302c2c','#e0d9d9'])
fig.update_layout(title='pe backtest heatmap by industry',
                  xaxis=dict(title='industry',side='bottom'),
                  yaxis=dict(title='multiple',),
                  )
fig.show()
category = ['食品工業','紡織纖維','航運業','油電燃氣業','橡膠工業']
data.set_universe(category=category)
pe = data.get('price_earning_ratio:本益比')
position = (pe>10) & (pe<15)
ind_report = sim(position=position, resample='M', name="策略教學範例:pe_industry_factor", upload=True)
ind_y_return=ind_report.creturn.calc_stats().return_table['YTD']
y_return=report.creturn.calc_stats().return_table['YTD']
print(ind_y_return-y_return)
print('better_ratio:',sum(ind_y_return>y_return)/len(ind_y_return))
