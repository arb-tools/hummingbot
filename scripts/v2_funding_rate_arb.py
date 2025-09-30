# 资费套利策略脚本 - 通过不同交易所的资费率差异进行套利交易
import os
from decimal import Decimal
from typing import Dict, List, Set

import pandas as pd
from pydantic import Field, field_validator

from hummingbot.client.ui.interface_utils import format_df_for_printout
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PriceType, TradeType
from hummingbot.core.event.events import FundingPaymentCompletedEvent
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


class FundingRateArbitrageConfig(StrategyV2ConfigBase):
    """资费率套利策略配置类"""
    script_file_name: str = os.path.basename(__file__)
    candles_config: List[CandlesConfig] = []
    controllers_config: List[str] = []
    markets: Dict[str, Set[str]] = {}
    leverage: int = Field(  # 杠杆倍数设置
        default=20, gt=0,
        json_schema_extra={"prompt": lambda mi: "Enter the leverage (e.g. 20): ", "prompt_on_new": True},
    )
    min_funding_rate_profitability: Decimal = Field(  # 进入仓位的最小资费率盈利能力阈值
        default=0.001,
        json_schema_extra={
            "prompt": lambda mi: "Enter the min funding rate profitability to enter in a position (e.g. 0.001): ",
            "prompt_on_new": True}
    )
    connectors: Set[str] = Field(  # 参与套利的交易所连接器配置
        default="hyperliquid_perpetual,binance_perpetual",
        json_schema_extra={
            "prompt": lambda mi: "Enter the connectors separated by commas (e.g. hyperliquid_perpetual,binance_perpetual): ",
            "prompt_on_new": True}
    )
    tokens: Set[str] = Field(
        default="WIF,FET",
        json_schema_extra={"prompt": lambda mi: "Enter the tokens separated by commas (e.g. WIF,FET): ", "prompt_on_new": True},
    )
    position_size_quote: Decimal = Field(  # 仓位大小(以计价货币计算)
        default=100,
        json_schema_extra={
            "prompt": lambda mi: "Enter the position size in quote asset (e.g. order amount 100 will open 100 long on hyperliquid and 100 short on binance): ",
            "prompt_on_new": True
        }
    )
    profitability_to_take_profit: Decimal = Field(  # 止盈盈利能力阈值(包括仓位盈亏和资费收入)
        default=0.01,
        json_schema_extra={
            "prompt": lambda mi: "Enter the profitability to take profit (including PNL of positions and fundings received): ",
            "prompt_on_new": True}
    )
    funding_rate_diff_stop_loss: Decimal = Field(
        default=-0.001,
        json_schema_extra={
            "prompt": lambda mi: "Enter the funding rate difference to stop the position (e.g. -0.001): ",
            "prompt_on_new": True}
    )
    trade_profitability_condition_to_enter: bool = Field(
        default=False,
        json_schema_extra={
            "prompt": lambda mi: "Do you want to check the trade profitability condition to enter? (True/False): ",
            "prompt_on_new": True}
    )

    @field_validator("connectors", "tokens", mode="before")
    @classmethod
    def validate_sets(cls, v):
        if isinstance(v, str):
            return set(v.split(","))
        return v


class FundingRateArbitrage(StrategyV2Base):
    """资费率套利策略主类"""
    # 各交易所计价货币映射
    quote_markets_map = {
        "hyperliquid_perpetual": "USD",
        "binance_perpetual": "USDT"
    }
    # 各交易所资费支付间隔时间映射(秒)
    funding_payment_interval_map = {
        "binance_perpetual": 60 * 60 * 1,  # 币安：1小时
        "hyperliquid_perpetual": 60 * 60 * 1  # Hyperliquid：1小时
    }
    funding_profitability_interval = 60 * 60 * 1  # 资费盈利能力计算间隔：24小时

    @classmethod
    def get_trading_pair_for_connector(cls, token, connector):
        return f"{token}-{cls.quote_markets_map.get(connector, 'USDT')}"

    @classmethod
    def init_markets(cls, config: FundingRateArbitrageConfig):
        markets = {}
        for connector in config.connectors:
            trading_pairs = {cls.get_trading_pair_for_connector(token, connector) for token in config.tokens}
            markets[connector] = trading_pairs
        cls.markets = markets

    def __init__(self, connectors: Dict[str, ConnectorBase], config: FundingRateArbitrageConfig):
        super().__init__(connectors, config)
        self.config = config
        self.active_funding_arbitrages = {}  # 活跃的资费套利仓位字典
        self.stopped_funding_arbitrages = {token: [] for token in self.config.tokens}  # 已停止的套利仓位记录

    def start(self, clock: Clock, timestamp: float) -> None:
        """
        Start the strategy.
        :param clock: Clock to use.
        :param timestamp: Current time.
        """
        self._last_timestamp = timestamp
        self.apply_initial_setting()

    def apply_initial_setting(self):
        """应用初始设置：配置仓位模式和杠杆"""
        for connector_name, connector in self.connectors.items():
            if self.is_perpetual(connector_name):
                # 根据交易所设置仓位模式：Hyperliquid使用单向，其他使用对冲模式
                position_mode = PositionMode.ONEWAY if connector_name == "hyperliquid_perpetual" else PositionMode.HEDGE
                connector.set_position_mode(position_mode)
                # 为所有交易对设置杠杆倍数
                for trading_pair in self.market_data_provider.get_trading_pairs(connector_name):
                    connector.set_leverage(trading_pair, self.config.leverage)

    def get_funding_info_by_token(self, token):
        """
        获取指定代币在所有交易所的资费率信息
        This method provides the funding rates across all the connectors
        """
        funding_rates = {}
        for connector_name, connector in self.connectors.items():
            trading_pair = self.get_trading_pair_for_connector(token, connector_name)
            funding_rates[connector_name] = connector.get_funding_info(trading_pair)  # 获取资费率信息
        return funding_rates

    def get_current_profitability_after_fees(self, token: str, connector_1: str, connector_2: str, side: TradeType):
        """
        计算扣除手续费后的当前交易盈利能力
        This methods compares the profitability of buying at market in the two exchanges. If the side is TradeType.BUY
        means that the operation is long on connector 1 and short on connector 2.
        """
        trading_pair_1 = self.get_trading_pair_for_connector(token, connector_1)
        trading_pair_2 = self.get_trading_pair_for_connector(token, connector_2)

        # 获取交易所1的执行价格（根据交易方向和仓位大小）
        connector_1_price = Decimal(self.market_data_provider.get_price_for_quote_volume(
            connector_name=connector_1,
            trading_pair=trading_pair_1,
            quote_volume=self.config.position_size_quote,
            is_buy=side == TradeType.BUY,
        ).result_price)
        # 获取交易所2的执行价格（与交易所1相反方向）
        connector_2_price = Decimal(self.market_data_provider.get_price_for_quote_volume(
            connector_name=connector_2,
            trading_pair=trading_pair_2,
            quote_volume=self.config.position_size_quote,
            is_buy=side != TradeType.BUY,
        ).result_price)
        # 计算交易所1的预估手续费
        estimated_fees_connector_1 = self.connectors[connector_1].get_fee(
            base_currency=trading_pair_1.split("-")[0],
            quote_currency=trading_pair_1.split("-")[1],
            order_type=OrderType.MARKET,
            order_side=TradeType.BUY,
            amount=self.config.position_size_quote / connector_1_price,
            price=connector_1_price,
            is_maker=False,
            position_action=PositionAction.OPEN
        ).percent
        # 计算交易所2的预估手续费
        estimated_fees_connector_2 = self.connectors[connector_2].get_fee(
            base_currency=trading_pair_2.split("-")[0],
            quote_currency=trading_pair_2.split("-")[1],
            order_type=OrderType.MARKET,
            order_side=TradeType.BUY,
            amount=self.config.position_size_quote / connector_2_price,
            price=connector_2_price,
            is_maker=False,
            position_action=PositionAction.OPEN
        ).percent

        # 根据交易方向计算价差盈利百分比
        if side == TradeType.BUY:
            estimated_trade_pnl_pct = (connector_2_price - connector_1_price) / connector_1_price
        else:
            estimated_trade_pnl_pct = (connector_1_price - connector_2_price) / connector_2_price
        # 返回扣除双边手续费后的净盈利能力
        return estimated_trade_pnl_pct - estimated_fees_connector_1 - estimated_fees_connector_2

    def get_most_profitable_combination(self, funding_info_report: Dict):
        """获取最有利可图的资费率套利组合"""
        best_combination = None
        highest_profitability = 0
        # 遍历所有交易所组合
        for connector_1 in funding_info_report:
            for connector_2 in funding_info_report:
                if connector_1 != connector_2:
                    # 获取标准化的每秒资费率
                    rate_connector_1 = self.get_normalized_funding_rate_in_seconds(funding_info_report, connector_1)
                    rate_connector_2 = self.get_normalized_funding_rate_in_seconds(funding_info_report, connector_2)
                    # 计算资费率差异的绝对值（24小时盈利能力）
                    funding_rate_diff = abs(rate_connector_1 - rate_connector_2) * self.funding_profitability_interval
                    if funding_rate_diff > highest_profitability:
                        # 确定交易方向：资费率低的做多，资费率高的做空
                        trade_side = TradeType.BUY if rate_connector_1 < rate_connector_2 else TradeType.SELL
                        highest_profitability = funding_rate_diff
                        best_combination = (connector_1, connector_2, trade_side, funding_rate_diff)
        return best_combination

    def get_normalized_funding_rate_in_seconds(self, funding_info_report, connector_name):
        return funding_info_report[connector_name].rate / self.funding_payment_interval_map.get(connector_name, 60 * 60 * 8)

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        """
        创建操作提案：评估是否需要为没有活跃套利的代币创建新的仓位组合
        In this method we are going to evaluate if a new set of positions has to be created for each of the tokens that
        don't have an active arbitrage.
        More filters can be applied to limit the creation of the positions, since the current logic is only checking for
        positive pnl between funding rate. Is logged and computed the trading profitability at the time for entering
        at market to open the possibilities for other people to create variations like sending limit position executors
        and if one gets filled buy market the other one to improve the entry prices.
        """
        create_actions = []
        # 遍历所有配置的代币
        for token in self.config.tokens:
            if token not in self.active_funding_arbitrages:  # 检查是否已有活跃套利
                # 获取该代币的资费率信息报告
                funding_info_report = self.get_funding_info_by_token(token)
                # 找到最优套利组合
                best_combination = self.get_most_profitable_combination(funding_info_report)
                connector_1, connector_2, trade_side, expected_profitability = best_combination
                # 检查预期盈利能力是否达到最小阈值
                if expected_profitability >= self.config.min_funding_rate_profitability:
                    # 计算当前扣除手续费后的盈利能力
                    current_profitability = self.get_current_profitability_after_fees(
                        token, connector_1, connector_2, trade_side
                    )
                    # 如果启用了交易盈利能力条件检查
                    if self.config.trade_profitability_condition_to_enter:
                        if current_profitability < 0:
                            self.logger().info(f"最佳组合: {connector_1} | {connector_2} | {trade_side}"
                                               f"资费率盈利能力: {expected_profitability}"
                                               f"扣费后交易盈利能力: {current_profitability}"
                                               f"交易盈利能力为负，跳过...")
                            continue
                    self.logger().info(f"最佳组合: {connector_1} | {connector_2} | {trade_side}"
                                       f"资费率盈利能力: {expected_profitability}"
                                       f"扣费后交易盈利能力: {current_profitability}"
                                       f"启动执行器...")
                    # 获取两个仓位执行器的配置
                    position_executor_config_1, position_executor_config_2 = self.get_position_executors_config(token, connector_1, connector_2, trade_side)
                    # 记录活跃的资费套利信息
                    self.active_funding_arbitrages[token] = {
                        "connector_1": connector_1,
                        "connector_2": connector_2,
                        "executors_ids": [position_executor_config_1.id, position_executor_config_2.id],
                        "side": trade_side,
                        "funding_payments": [],  # 资费支付记录
                    }
                    # 返回创建执行器的操作
                    return [CreateExecutorAction(executor_config=position_executor_config_1),
                            CreateExecutorAction(executor_config=position_executor_config_2)]
        return create_actions

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        """
        停止操作提案：监控资费套利的盈亏和当前执行器盈亏，决定是否平仓
        Once the funding rate arbitrage is created we are going to control the funding payments pnl and the current
        pnl of each of the executors at the cost of closing the open position at market.
        If that PNL is greater than the profitability_to_take_profit
        """
        stop_executor_actions = []
        # 遍历所有活跃的资费套利
        for token, funding_arbitrage_info in self.active_funding_arbitrages.items():
            # 筛选出属于当前套利的执行器
            executors = self.filter_executors(
                executors=self.get_all_executors(),
                filter_func=lambda x: x.id in funding_arbitrage_info["executors_ids"]
            )
            # 计算资费支付总盈亏
            funding_payments_pnl = sum(funding_payment.amount for funding_payment in funding_arbitrage_info["funding_payments"])
            # 计算执行器总盈亏
            executors_pnl = sum(executor.net_pnl_quote for executor in executors)
            # 检查是否达到止盈条件
            take_profit_condition = executors_pnl + funding_payments_pnl > self.config.profitability_to_take_profit * self.config.position_size_quote
            # 获取当前资费率信息
            funding_info_report = self.get_funding_info_by_token(token)
            # 根据交易方向计算资费率差异
            if funding_arbitrage_info["side"] == TradeType.BUY:
                funding_rate_diff = self.get_normalized_funding_rate_in_seconds(funding_info_report, funding_arbitrage_info["connector_2"]) - self.get_normalized_funding_rate_in_seconds(funding_info_report, funding_arbitrage_info["connector_1"])
            else:
                funding_rate_diff = self.get_normalized_funding_rate_in_seconds(funding_info_report, funding_arbitrage_info["connector_1"]) - self.get_normalized_funding_rate_in_seconds(funding_info_report, funding_arbitrage_info["connector_2"])
            # 检查当前资费率差异是否触发止损条件
            current_funding_condition = funding_rate_diff * self.funding_profitability_interval < self.config.funding_rate_diff_stop_loss
            # 执行止盈操作
            if take_profit_condition:
                self.logger().info("达到止盈盈利能力，停止执行器")
                self.stopped_funding_arbitrages[token].append(funding_arbitrage_info)
                stop_executor_actions.extend([StopExecutorAction(executor_id=executor.id) for executor in executors])
            # 执行资费率差异止损操作
            elif current_funding_condition:
                self.logger().info("资费率差异达到止损条件，停止执行器")
                self.stopped_funding_arbitrages[token].append(funding_arbitrage_info)
                stop_executor_actions.extend([StopExecutorAction(executor_id=executor.id) for executor in executors])
        return stop_executor_actions

    def did_complete_funding_payment(self, funding_payment_completed_event: FundingPaymentCompletedEvent):
        """
        处理资费支付完成事件：检查是否匹配活跃套利并添加到记录中
        Based on the funding payment event received, check if one of the active arbitrages matches to add the event
        to the list.
        """
        # 从交易对中提取代币名称
        token = funding_payment_completed_event.trading_pair.split("-")[0]
        # 如果该代币有活跃套利，则记录资费支付事件
        if token in self.active_funding_arbitrages:
            self.active_funding_arbitrages[token]["funding_payments"].append(funding_payment_completed_event)

    def get_position_executors_config(self, token, connector_1, connector_2, trade_side):
        """获取仓位执行器配置"""
        # 获取中间价作为参考价格
        price = self.market_data_provider.get_price_by_type(
            connector_name=connector_1,
            trading_pair=self.get_trading_pair_for_connector(token, connector_1),
            price_type=PriceType.MidPrice
        )
        # 根据仓位大小和价格计算交易数量
        position_amount = self.config.position_size_quote / price

        # 创建交易所1的仓位执行器配置
        position_executor_config_1 = PositionExecutorConfig(
            timestamp=self.current_timestamp,
            connector_name=connector_1,
            trading_pair=self.get_trading_pair_for_connector(token, connector_1),
            side=trade_side,
            amount=position_amount,
            leverage=self.config.leverage,
            triple_barrier_config=TripleBarrierConfig(open_order_type=OrderType.MARKET),  # 使用市价单开仓
        )
        # 创建交易所2的仓位执行器配置（方向相反）
        position_executor_config_2 = PositionExecutorConfig(
            timestamp=self.current_timestamp,
            connector_name=connector_2,
            trading_pair=self.get_trading_pair_for_connector(token, connector_2),
            side=TradeType.BUY if trade_side == TradeType.SELL else TradeType.SELL,  # 相反交易方向
            amount=position_amount,
            leverage=self.config.leverage,
            triple_barrier_config=TripleBarrierConfig(open_order_type=OrderType.MARKET),  # 使用市价单开仓
        )
        return position_executor_config_1, position_executor_config_2

    def format_status(self) -> str:
        """格式化策略状态显示信息"""
        original_status = super().format_status()
        funding_rate_status = []
        if self.ready_to_trade:
            all_funding_info = []  # 所有代币的资费率信息
            all_best_paths = []    # 所有代币的最佳套利路径
            # 遍历所有配置的代币
            for token in self.config.tokens:
                token_info = {"token": token}  # 代币基础信息
                best_paths_info = {"token": token}  # 最佳路径信息
                # 获取该代币的资费率报告
                funding_info_report = self.get_funding_info_by_token(token)
                # 获取最佳套利组合
                best_combination = self.get_most_profitable_combination(funding_info_report)
                # 为每个交易所添加标准化的资费率信息（24小时计算）
                for connector_name, info in funding_info_report.items():
                    token_info[f"{connector_name} Rate (%)"] = self.get_normalized_funding_rate_in_seconds(funding_info_report, connector_name) * self.funding_profitability_interval * 100
                connector_1, connector_2, side, funding_rate_diff = best_combination
                # 计算扣除手续费后的交易盈利能力
                profitability_after_fees = self.get_current_profitability_after_fees(token, connector_1, connector_2, side)
                best_paths_info["Best Path"] = f"{connector_1}_{connector_2}"  # 最佳套利路径
                best_paths_info["Best Rate Diff (%)"] = funding_rate_diff * 100  # 最佳资费率差异
                best_paths_info["Trade Profitability (%)"] = profitability_after_fees * 100  # 交易盈利能力
                best_paths_info["Days Trade Prof"] = - profitability_after_fees / funding_rate_diff  # 交易盈利能力相当于多少天
                best_paths_info["Days to TP"] = (self.config.profitability_to_take_profit - profitability_after_fees) / funding_rate_diff  # 达到止盈需要的天数

                # 计算距离下次资费支付的时间
                time_to_next_funding_info_c1 = funding_info_report[connector_1].next_funding_utc_timestamp - self.current_timestamp
                time_to_next_funding_info_c2 = funding_info_report[connector_2].next_funding_utc_timestamp - self.current_timestamp
                best_paths_info["Min to Funding 1"] = time_to_next_funding_info_c1 / 60  # 交易所1距下次资费支付分钟数
                best_paths_info["Min to Funding 2"] = time_to_next_funding_info_c2 / 60  # 交易所2距下次资费支付分钟数

                all_funding_info.append(token_info)
                all_best_paths.append(best_paths_info)
            funding_rate_status.append(f"\n\n\nMin Funding Rate Profitability: {self.config.min_funding_rate_profitability:.2%}")
            funding_rate_status.append(f"Profitability to Take Profit: {self.config.profitability_to_take_profit:.2%}\n")
            funding_rate_status.append("Funding Rate Info (Funding Profitability in Days): ")
            funding_rate_status.append(format_df_for_printout(df=pd.DataFrame(all_funding_info), table_format="psql",))
            funding_rate_status.append(format_df_for_printout(df=pd.DataFrame(all_best_paths), table_format="psql",))
            # 显示活跃的资费套利信息
            for token, funding_arbitrage_info in self.active_funding_arbitrages.items():
                # 确定哪个交易所做多，哪个做空
                long_connector = funding_arbitrage_info["connector_1"] if funding_arbitrage_info["side"] == TradeType.BUY else funding_arbitrage_info["connector_2"]
                short_connector = funding_arbitrage_info["connector_2"] if funding_arbitrage_info["side"] == TradeType.BUY else funding_arbitrage_info["connector_1"]
                funding_rate_status.append(f"代币: {token}")
                funding_rate_status.append(f"做多交易所: {long_connector} | 做空交易所: {short_connector}")
                funding_rate_status.append(f"已收集资费支付: {funding_arbitrage_info['funding_payments']}")
                funding_rate_status.append(f"执行器ID: {funding_arbitrage_info['executors_ids']}")
                funding_rate_status.append("-" * 50 + "\n")
        return original_status + "\n".join(funding_rate_status)
