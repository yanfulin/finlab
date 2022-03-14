from finlab.shioaji_order_executor.utils import transfer_order_datetime
from finlab.shioaji_order_executor.account_admin import AccountAdmin
from finlab.shioaji_order_executor.strategy_admin import StrategyAdmin
import pandas as pd
import math
import os
from datetime import date
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from jupyter_dash import JupyterDash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
from abc import ABC, abstractmethod


class PlotBase(ABC):
    def __init__(self, api=None):
        self.api = api
        self.strategy_pkl_path = os.getenv('STRATEGY_PKL_PATH')
        self.orders_pkl_path = os.getenv('ORDERS_PKL_PATH')
        self.finlab_api_token = os.getenv('FINLAB_API_TOKEN')

    @abstractmethod
    def preprocess_data(self):
        pass

    @abstractmethod
    def create_fig(self, start_date, end_date, value):
        pass

    def plot_dash(self, strategy_options=True):
        # Build App
        all_option = {'label': 'all', 'value': 'all'}
        if strategy_options:
            strategy_list = list(set(pd.read_pickle(self.strategy_pkl_path)['name'].values))
            options = [{'label': s, 'value': s} for s in strategy_list]
            options.append(all_option)
            options = sorted(options, key=lambda k: k['label'])
        else:
            options = [all_option]

        app = JupyterDash(__name__)
        app.layout = html.Div([
            html.H1("Finlab JupyterDash"),
            html.P("date_range:"),
            dcc.DatePickerRange(
                id='my-date-picker-range',
                min_date_allowed=date(1990, 1, 1),
                max_date_allowed=date(2100, 12, 31),
                initial_visible_month=date(2021, 1, 1),
                start_date=date(2021, 1, 1),
                end_date=date(2021, 12, 31)
            ),
            html.P("strategy:"),
            dcc.Dropdown(
                id='dropdown',
                options=options,
                value='all',
                style={'width': '50%'}
            ),
            dcc.Graph(id="graph")
        ])

        @app.callback(
            Output("graph", "figure"),
            [Input('my-date-picker-range', 'start_date'),
             Input('my-date-picker-range', 'end_date'),
             Input('dropdown', 'value')])
        def update_output(start_date, end_date, value):
            return self.create_fig(start_date, end_date, value)

        # Run app and display result inline in the notebook
        app.run_server(mode='inline')


class RealizedProfitLoss(PlotBase):

    def preprocess_data(self, start_date=None, end_date=None):
        # orders_df
        df = pd.read_pickle(self.orders_pkl_path)
        df = df[df['status_code'] == '00']
        df['date'] = df['order_datetime'].apply(lambda s: transfer_order_datetime(s))
        df = df.set_index(['date', 'ordno', 'code'])

        # pl_df
        pl_df = pd.DataFrame(self.api.list_profit_loss(self.api.stock_account, start_date, end_date))
        pl_df['ordno'] = pl_df['dseq'].apply(lambda s: s[:5])
        pl_df['date'] = pd.to_datetime(pl_df['date'])
        pl_df = pl_df.groupby(['date', 'ordno', 'code'])[['pnl']].sum()
        for col in ['strategy', 'name']:
            pl_df[col] = df[col]
        pl_df = pl_df.reset_index()
        pl_df['strategy'] = pl_df['strategy'].fillna('nan')
        pl_df['stock_id'] = pl_df['code']
        return pl_df

    def create_fig(self, start_date=None, end_date=None, strategy='all'):

        df = self.preprocess_data(start_date, end_date)
        if strategy != 'all':
            df = df[df['strategy'] == strategy]
        if start_date:
            df = df[df['date'] >= start_date]
        if end_date:
            df = df[df['date'] <= end_date]
        date_group = df.groupby(['date'])[['pnl']].sum()
        df = df.groupby(['stock_id'])[['pnl']].sum()
        df = df.reset_index()
        df = df.sort_values(['pnl'])
        df['category'] = ['profit' if i > 0 else 'loss' for i in df['pnl'].values]
        df['pnl_absolute_value'] = [i if i > 0 else i * -1 for i in df['pnl'].values]

        df_category = df.groupby(['category'])[['pnl_absolute_value']].sum()
        df_category = df_category.reset_index()
        df_category = df_category.rename(columns={'category': 'stock_id'})
        df_category['category'] = 'total'

        df_total = pd.DataFrame(
            {'stock_id': 'total', 'pnl_absolute_value': df['pnl_absolute_value'].sum(), 'category': ''},
            index=[0])
        df_all = pd.concat([df, df_category, df_total])

        labels = df['stock_id']

        # Create subplots: use 'domain' type for Pie subplot
        fig = make_subplots(rows=4,
                            cols=3,
                            specs=[[{'type': 'domain', "rowspan": 2}, {'type': 'domain', "rowspan": 2},
                                    {'type': 'domain', "rowspan": 2}],
                                   [None, None, None],
                                   [{'type': 'xy', "colspan": 3, "secondary_y": True}, None, None],
                                   [{'type': 'xy', "colspan": 3}, None, None]],
                            horizontal_spacing=0.03,
                            vertical_spacing=0.08,
                            subplot_titles=('Profit Pie: ' + str(df[df['pnl'] > 0]['pnl'].sum()),
                                            'Loss Pie: ' + str(df[df['pnl'] < 0]['pnl'].sum()),
                                            'Profit Loss Sunburst: ' + str(df['pnl'].sum()),
                                            'Profit Loss Bar By Date',
                                            'Profit Loss Bar By Target',
                                            )
                            )
        fig.add_trace(go.Pie(labels=labels, values=df['pnl'], name="profit", hole=.3, textposition='inside',
                             textinfo='percent+label'), 1, 1)
        fig.add_trace(go.Pie(labels=labels, values=df[df['pnl'] < 0]['pnl'] * -1, name="loss", hole=.3,
                             textposition='inside', textinfo='percent+label'), 1, 2)
        fig.add_trace(go.Sunburst(
            labels=df_all.stock_id,
            parents=df_all.category,
            values=df_all.pnl_absolute_value,
            branchvalues='total',
            marker=dict(
                colors=df_all.pnl_absolute_value.apply(lambda s: math.log(s + 0.1)),
                colorscale='earth'),
            textinfo='label+percent entry',
        ), 1, 3)
        fig.add_trace(go.Bar(x=date_group.index, y=date_group['pnl'], name="date", marker_color="#636EFA"), 3, 1)
        fig.add_trace(
            go.Scatter(x=date_group.index, y=date_group['pnl'].cumsum(), name="cumsum_realized_pnl",
                       marker_color="#FFA15A"),
            secondary_y=True, row=3, col=1)
        fig.add_trace(go.Bar(x=df['stock_id'], y=df['pnl'], name="stock_id", marker_color="#636EFA"), 4, 1)

        fig['layout']['yaxis']['title'] = '$NTD'
        fig['layout']['yaxis2']['showgrid'] = False
        fig['layout']['yaxis2']['title'] = '$NTD(cumsum)'
        fig['layout']['yaxis3']['title'] = '$NTD'

        fig.update_layout(
            title={
                'text': f"Realized Profit Loss Statistic ({start_date}~{end_date})",
                'x': 0.49,
                'y': 0.99,
                'xanchor': 'center',
                'yanchor': 'top'},
            paper_bgcolor='rgb(233, 243, 243)',
            width=1200,
            height=1000)
        return fig


class ShioajiStockAssetDashboard(PlotBase):

    def preprocess_data(self, strategy='all'):
        account_admin = AccountAdmin(self.api)
        strategy_admin = StrategyAdmin(self.api)
        account_settlement = account_admin.get_account_settlement()
        account_positions_cost = account_admin.get_positions_cost()
        strategy_accounting_info = strategy_admin.get_strategy_account_info()
        strategy_daily_info = strategy_admin.get_strategy_daily_info(strategy)
        strategy_daily_info = pd.DataFrame(strategy_daily_info)
        recent_daily_info = pd.DataFrame(strategy_daily_info['data'].iloc[-1])
        recent_daily_info = recent_daily_info[recent_daily_info['hold_quantity'] > 0]
        recent_daily_info['strategy'] = strategy

        return account_settlement, account_positions_cost, strategy_accounting_info, strategy_daily_info, recent_daily_info

    def create_fig(self, start_date=None, end_date=None, strategy='all'):
        account_settlement, account_positions_cost, strategy_accounting_info, strategy_daily_info, recent_daily_info = \
            self.preprocess_data(strategy)

        fig = make_subplots(rows=4,
                            cols=3,
                            specs=[[{'type': 'table'}, {'type': 'table'}, {'type': 'table'}],
                                   [{'type': 'domain'}, {'type': 'domain'}, {'type': 'domain'}],
                                   [{'type': 'xy', "colspan": 2}, None, {'type': 'domain'}],
                                   [{'type': 'xy', "colspan": 2}, None, {'type': 'table'}]],
                            horizontal_spacing=0.03,
                            vertical_spacing=0.05,
                            subplot_titles=(
                                'Account positions:total pnl is ' + str(account_positions_cost['pnl'].sum()),
                                'Account settlement',
                                'Strategy size info: avalible size balance is ' + str(
                                    account_positions_cost['net_value'].sum() - strategy_accounting_info['size'].sum()),
                                'Asset net value: ' + str(account_positions_cost['net_value'].sum()),
                                'Asset cost Value: ' + str(account_positions_cost['cost'].iloc[:-1].sum()),
                                'Strategy size',
                                'Daily change in cost of holdings: ' + strategy,
                                'Strategy hold asset: ' + strategy,
                                'Daily change in cost of pnl: ' + strategy,
                                'Strategy hold asset detail: ' + strategy,
                            )
                            )

        fig.add_trace(go.Table(
            header=dict(values=account_positions_cost.columns,
                        line_color='darkslategray',
                        fill_color='royalblue',
                        align=['left', 'center'],
                        font=dict(color='white', size=10)),
            cells=dict(values=[account_positions_cost[i] for i in account_positions_cost.columns],
                       fill_color='lightcyan',
                       align='left')), row=1, col=1)

        fig.add_trace(go.Table(
            header=dict(values=account_settlement.columns,
                        line_color='darkslategray',
                        fill_color='royalblue',
                        align=['left', 'center'],
                        font=dict(color='white', size=10)),
            cells=dict(values=[account_settlement[i] for i in account_settlement.columns],
                       line_color='darkslategray',
                       fill_color='lightcyan',
                       align='left')), row=1, col=2)

        fig.add_trace(go.Table(
            header=dict(values=strategy_accounting_info.columns,
                        line_color='darkslategray',
                        fill_color='royalblue',
                        align=['left', 'center'],
                        font=dict(color='white', size=10)),

            cells=dict(values=[strategy_accounting_info[i] for i in strategy_accounting_info.columns],
                       line_color='darkslategray',
                       fill_color='lightcyan',
                       align='left')), row=1, col=3)

        fig.add_trace(
            go.Pie(values=account_positions_cost['net_value'], labels=account_positions_cost['code'],
                   name="asset_ratio",
                   textinfo='label+percent'),
            row=2, col=1)

        fig.add_trace(
            go.Pie(values=account_positions_cost['cost'], labels=account_positions_cost['code'], name="asset_ratio",
                   textinfo='label+percent'),
            row=2, col=2)

        fig.add_trace(
            go.Pie(values=strategy_accounting_info['est_sell_net_value'], labels=strategy_accounting_info['name'],
                   name="strategy_asset_ratio",
                   textinfo='label+percent'),
            row=2, col=3)

        # hold_cost area plot
        fig.add_trace(
            go.Scatter(x=strategy_daily_info['date'], y=strategy_daily_info['hold_cost'], fill='tozeroy',
                       name="hold_cost",
                       marker_color="#FFA15A"),
            row=3, col=1)

        fig.add_trace(
            go.Pie(values=recent_daily_info['net_value'], labels=recent_daily_info['stock_id'],
                   name="Strategy net value",
                   textinfo='label+percent'),
            row=3, col=3)

        #  realized_cumsum_pnl,hold est_sell_pnl plot
        fig.add_trace(go.Scatter(x=strategy_daily_info['date'], y=strategy_daily_info['realized_pnl'], fill='tozeroy',
                                 line=dict(width=0.5, color='rgb(249, 135, 66)'),
                                 name='realized_pnl'), 4, 1)
        fig.add_trace(go.Scatter(x=strategy_daily_info['date'], y=strategy_daily_info['est_sell_pnl'], fill='tozeroy',
                                 line=dict(width=0.5, color='rgb(0, 121, 41)'),
                                 name='est_unrealized_pnl'), 4, 1)
        fig.add_trace(go.Scatter(x=strategy_daily_info['date'],
                                 y=strategy_daily_info['realized_pnl'] + strategy_daily_info['est_sell_pnl'],
                                 fill='tozeroy', line=dict(width=0.5, color='rgb(0, 126, 196)'),
                                 name='total_pnl'), 4, 1)

        fig.add_trace(go.Table(
            header=dict(values=recent_daily_info.columns,
                        line_color='darkslategray',
                        fill_color='royalblue',
                        align=['left', 'center'],
                        font=dict(color='white', size=10)),
            cells=dict(values=[recent_daily_info[i] for i in recent_daily_info.columns],
                       fill_color='lightcyan',
                       align='left')), row=4, col=3)

        fig.update_layout(
            title={
                'text': f"Shioaji Stock Asset Dashboard",
                'x': 0.49,
                'y': 0.99,
                'xanchor': 'center',
                'yanchor': 'top'},
            paper_bgcolor='rgb(233, 243, 243)',
            width=1700,
            height=1500)
        return fig