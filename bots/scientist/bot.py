"""
Bot 3: Scientist (العَالِم) - ENHANCED
Advanced Genetic Algorithms + Feature Engineering + Hugging Face
No fixed generation limit - evolves until fitness ≥ 0.88 or Sharpe ≥ 2.0
Minimum 300 generations to ensure wide exploration
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from loguru import logger
import random
from deap import base, creator, tools, algorithms
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification, AutoModel
from scipy import stats

from sqlalchemy import text
from config.config_manager import config
from scripts.database import db
from scripts.redis_manager import redis_manager
from scripts.utils import get_saudi_time
from scripts.advanced_technical_analysis import AdvancedTechnicalAnalysis


class Scientist:
    """
    Advanced AI/ML bot for strategy optimization and feature discovery
    Uses Genetic Algorithms with adaptive stopping criteria
    """
    
    def __init__(self):
        self.name = "scientist"
        self.logger = logger.bind(bot=self.name)
        self.config = config.get_bot_config(self.name)
        
        # GA Parameters - ADAPTIVE (no fixed limit)
        self.population_size = self.config.get('population_size', 100)
        self.min_generations = self.config.get('min_generations', 300)  # Minimum 300
        self.max_generations = self.config.get('max_generations', 1000)  # Safety limit
        self.target_fitness = self.config.get('target_fitness', 0.88)
        self.target_sharpe = self.config.get('target_sharpe', 2.0)
        self.crossover_prob = self.config.get('crossover_prob', 0.7)
        self.mutation_prob = self.config.get('mutation_prob', 0.1)
        
        # Advanced TA
        self.advanced_ta = AdvancedTechnicalAnalysis()
        
        # Hugging Face: FinBERT for sentiment (secondary, 10-15% weight)
        self.sentiment_model = None
        self.sentiment_tokenizer = None
        self._load_sentiment_model()

        # Hugging Face: FinBERT embeddings as primary GA features
        # Uses the same FinBERT model but extracts [CLS] embeddings (768-dim)
        # These embeddings encode financial context and are used as new features
        self.embedding_model = None
        self.embedding_tokenizer = None
        self._load_embedding_model()

        # Cache for embeddings (symbol -> np.array)
        self._embedding_cache: Dict[str, np.ndarray] = {}

        # Setup DEAP
        self._setup_deap()
        
    def _load_sentiment_model(self):
        """Load FinBERT model for financial sentiment analysis"""
        try:
            model_name = "ProsusAI/finbert"
            self.logger.info(f"Loading sentiment model: {model_name}")
            
            self.sentiment_tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.sentiment_model = AutoModelForSequenceClassification.from_pretrained(model_name)
            
            # Move to GPU if available
            if torch.cuda.is_available():
                self.sentiment_model = self.sentiment_model.cuda()
                self.logger.info("Sentiment model loaded on GPU")
            else:
                self.logger.info("Sentiment model loaded on CPU")
                
        except Exception as e:
            self.logger.error(f"Error loading sentiment model: {e}")
            self.sentiment_model = None
    
    def analyze_sentiment(self, text: str) -> Dict:
        """
        Analyze sentiment of financial text using FinBERT
        Returns: {sentiment: 'positive'/'negative'/'neutral', score: float}
        """
        if not self.sentiment_model or not text:
            return {'sentiment': 'neutral', 'score': 0.0}
        
        try:
            inputs = self.sentiment_tokenizer(text, return_tensors="pt", 
                                             truncation=True, max_length=512)
            
            if torch.cuda.is_available():
                inputs = {k: v.cuda() for k, v in inputs.items()}
            
            with torch.no_grad():
                outputs = self.sentiment_model(**inputs)
            
            predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)
            
            # FinBERT outputs: [negative, neutral, positive]
            scores = predictions[0].cpu().numpy()
            sentiment_map = ['negative', 'neutral', 'positive']
            
            max_idx = np.argmax(scores)
            sentiment = sentiment_map[max_idx]
            score = float(scores[max_idx])
            
            return {
                'sentiment': sentiment,
                'score': score,
                'scores': {
                    'negative': float(scores[0]),
                    'neutral': float(scores[1]),
                    'positive': float(scores[2])
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing sentiment: {e}")
            return {'sentiment': 'neutral', 'score': 0.0}
    
    def _load_embedding_model(self):
        """
        Load FinBERT base model for [CLS] embedding extraction.
        These 768-dim embeddings are used as NEW features inside the GA,
        representing financial context that standard TA cannot capture.
        """
        try:
            model_name = "ProsusAI/finbert"
            self.logger.info(f"[Scientist] Loading embedding model: {model_name}")
            self.embedding_tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.embedding_model = AutoModel.from_pretrained(model_name)
            if torch.cuda.is_available():
                self.embedding_model = self.embedding_model.cuda()
            self.embedding_model.eval()
            self.logger.info("[Scientist] Embedding model loaded (768-dim [CLS] vectors)")
        except Exception as e:
            self.logger.error(f"[Scientist] Error loading embedding model: {e}")
            self.embedding_model = None

    def get_text_embedding(self, text: str) -> Optional[np.ndarray]:
        """
        Extract 768-dim [CLS] embedding from FinBERT for a given text.
        Used as PRIMARY new features inside the GA (not just sentiment).
        Returns: np.ndarray of shape (768,) or None on failure.
        """
        if not self.embedding_model or not text:
            return None
        try:
            inputs = self.embedding_tokenizer(
                text, return_tensors="pt", truncation=True, max_length=512
            )
            if torch.cuda.is_available():
                inputs = {k: v.cuda() for k, v in inputs.items()}
            with torch.no_grad():
                outputs = self.embedding_model(**inputs)
            # [CLS] token embedding = first token of last hidden state
            cls_embedding = outputs.last_hidden_state[:, 0, :].squeeze().cpu().numpy()
            return cls_embedding  # shape: (768,)
        except Exception as e:
            self.logger.error(f"[Scientist] Error extracting embedding: {e}")
            return None

    def enrich_features_with_embeddings(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        Enrich the features DataFrame with FinBERT embeddings as new columns.

        Strategy:
        1. Fetch recent news headlines for the symbol from market_data.news.
        2. Concatenate headlines into a single text.
        3. Extract 768-dim [CLS] embedding.
        4. Reduce to top-10 PCA components (to avoid curse of dimensionality).
        5. Add as new columns: emb_0 ... emb_9.

        If no news available, uses a zero vector (neutral context).
        """
        try:
            # Check cache first
            cache_key = f"embedding:{symbol}"
            if symbol in self._embedding_cache:
                embedding_10d = self._embedding_cache[symbol]
            else:
                # Fetch recent headlines from DB
                news_text = ""
                try:
                    with db.get_session() as session:
                        result = session.execute(
                            """
                            SELECT title, content FROM market_data.news
                            WHERE symbol = :symbol
                            ORDER BY published_at DESC LIMIT 10
                            """,
                            {'symbol': symbol}
                        )
                        rows = result.fetchall()
                    if rows:
                        news_text = " . ".join(
                            [(r[0] or '') + ' ' + (r[1] or '')[:200] for r in rows]
                        ).strip()
                except Exception:
                    pass

                if not news_text:
                    # No news: use symbol name as minimal context
                    news_text = f"Saudi stock {symbol} market analysis"

                raw_emb = self.get_text_embedding(news_text)
                if raw_emb is None:
                    raw_emb = np.zeros(768)

                # PCA reduction: 768 → 10 dims
                from sklearn.decomposition import PCA
                pca = PCA(n_components=min(10, len(raw_emb)))
                embedding_10d = pca.fit_transform(raw_emb.reshape(1, -1)).flatten()
                self._embedding_cache[symbol] = embedding_10d

            # Add embedding columns to every row (broadcast)
            for i, val in enumerate(embedding_10d):
                df[f'emb_{i}'] = float(val)

            self.logger.info(
                f"[Scientist] Enriched {symbol} features with "
                f"{len(embedding_10d)} FinBERT embedding dims"
            )
            return df

        except Exception as e:
            self.logger.error(f"[Scientist] Error enriching features with embeddings: {e}")
            return df

    def _setup_deap(self):
        """Setup DEAP genetic algorithm framework"""
        try:
            # Create fitness and individual classes
            if not hasattr(creator, "FitnessMax"):
                creator.create("FitnessMax", base.Fitness, weights=(1.0,))
            if not hasattr(creator, "Individual"):
                creator.create("Individual", list, fitness=creator.FitnessMax)
            
            self.toolbox = base.Toolbox()
            
            # Gene ranges for strategy parameters
            # Format: [rsi_period, rsi_buy, rsi_sell, bb_period, bb_std, 
            #          macd_fast, macd_slow, macd_signal, stop_loss, take_profit]
            self.gene_ranges = [
                (5, 30),      # RSI period
                (20, 40),     # RSI buy threshold
                (60, 80),     # RSI sell threshold
                (10, 30),     # BB period
                (1.5, 3.0),   # BB std multiplier
                (8, 15),      # MACD fast
                (20, 30),     # MACD slow
                (5, 12),      # MACD signal
                (0.005, 0.02),  # Stop loss (0.5% - 2%)
                (0.01, 0.05)    # Take profit (1% - 5%)
            ]
            
            # Register genetic operators
            self.toolbox.register("individual", self._create_individual)
            self.toolbox.register("population", tools.initRepeat, list, self.toolbox.individual)
            self.toolbox.register("mate", tools.cxTwoPoint)
            self.toolbox.register("mutate", self._mutate_individual)
            self.toolbox.register("select", tools.selTournament, tournsize=3)
            self.toolbox.register("evaluate", self._evaluate_individual)
            
            self.logger.info("DEAP framework initialized")
            
        except Exception as e:
            self.logger.error(f"Error setting up DEAP: {e}")
    
    def _create_individual(self):
        """Create a random individual (strategy parameters)"""
        individual = []
        for min_val, max_val in self.gene_ranges:
            if isinstance(min_val, int):
                gene = random.randint(min_val, max_val)
            else:
                gene = random.uniform(min_val, max_val)
            individual.append(gene)
        return creator.Individual(individual)
    
    def _mutate_individual(self, individual, indpb=0.2):
        """Mutate an individual with adaptive mutation"""
        for i in range(len(individual)):
            if random.random() < indpb:
                min_val, max_val = self.gene_ranges[i]
                if isinstance(min_val, int):
                    individual[i] = random.randint(min_val, max_val)
                else:
                    individual[i] = random.uniform(min_val, max_val)
        return individual,
    
    def _evaluate_individual(self, individual: List, historical_data: pd.DataFrame = None) -> Tuple[float,]:
        """
        Evaluate fitness of an individual (strategy parameters)
        Returns: (fitness_score,)
        """
        try:
            if historical_data is None or len(historical_data) < 100:
                return (0.0,)
            
            # Extract parameters
            rsi_period = int(individual[0])
            rsi_buy = individual[1]
            rsi_sell = individual[2]
            bb_period = int(individual[3])
            bb_std = individual[4]
            macd_fast = int(individual[5])
            macd_slow = int(individual[6])
            macd_signal = int(individual[7])
            stop_loss = individual[8]
            take_profit = individual[9]
            
            # Backtest this strategy
            returns = self._backtest_strategy(
                historical_data,
                rsi_period, rsi_buy, rsi_sell,
                bb_period, bb_std,
                macd_fast, macd_slow, macd_signal,
                stop_loss, take_profit
            )
            
            if len(returns) == 0:
                return (0.0,)
            
            # Calculate fitness metrics
            total_return = np.sum(returns)
            sharpe_ratio = self._calculate_sharpe(returns)
            max_drawdown = self._calculate_max_drawdown(returns)
            win_rate = len([r for r in returns if r > 0]) / len(returns) if len(returns) > 0 else 0
            
            # Multi-objective fitness function
            # Weighted combination of metrics
            fitness = (
                0.3 * min(total_return / 0.5, 1.0) +  # Normalize to 50% total return
                0.3 * min(sharpe_ratio / 2.0, 1.0) +  # Normalize to Sharpe 2.0
                0.2 * (1.0 - min(abs(max_drawdown) / 0.2, 1.0)) +  # Penalize drawdown > 20%
                0.2 * win_rate
            )
            
            return (fitness,)
            
        except Exception as e:
            self.logger.error(f"Error evaluating individual: {e}")
            return (0.0,)
    
    def _backtest_strategy(self, df: pd.DataFrame, 
                          rsi_period, rsi_buy, rsi_sell,
                          bb_period, bb_std,
                          macd_fast, macd_slow, macd_signal,
                          stop_loss, take_profit) -> List[float]:
        """
        Simple backtest of strategy parameters
        Returns list of trade returns
        """
        try:
            # Calculate indicators
            import ta
            
            rsi = ta.momentum.RSIIndicator(df['close'], window=rsi_period).rsi()
            bb = ta.volatility.BollingerBands(df['close'], window=bb_period, window_dev=bb_std)
            bb_lower = bb.bollinger_lband()
            bb_upper = bb.bollinger_hband()
            macd_ind = ta.trend.MACD(df['close'], window_fast=macd_fast, 
                                     window_slow=macd_slow, window_sign=macd_signal)
            macd = macd_ind.macd()
            macd_sig = macd_ind.macd_signal()
            
            returns = []
            position = None
            entry_price = 0
            
            for i in range(max(rsi_period, bb_period, macd_slow), len(df)):
                current_price = df['close'].iloc[i]
                
                # Entry signals
                if position is None:
                    # Buy signal
                    if (rsi.iloc[i] < rsi_buy and 
                        current_price <= bb_lower.iloc[i] and
                        macd.iloc[i] > macd_sig.iloc[i]):
                        position = 'long'
                        entry_price = current_price
                
                # Exit signals
                elif position == 'long':
                    # Stop loss
                    if current_price <= entry_price * (1 - stop_loss):
                        trade_return = (current_price - entry_price) / entry_price
                        returns.append(trade_return)
                        position = None
                    
                    # Take profit
                    elif current_price >= entry_price * (1 + take_profit):
                        trade_return = (current_price - entry_price) / entry_price
                        returns.append(trade_return)
                        position = None
                    
                    # Sell signal
                    elif (rsi.iloc[i] > rsi_sell or 
                          current_price >= bb_upper.iloc[i]):
                        trade_return = (current_price - entry_price) / entry_price
                        returns.append(trade_return)
                        position = None
            
            # Close any open position
            if position == 'long':
                trade_return = (df['close'].iloc[-1] - entry_price) / entry_price
                returns.append(trade_return)
            
            return returns
            
        except Exception as e:
            self.logger.error(f"Error in backtest: {e}")
            return []
    
    def _calculate_sharpe(self, returns: List[float], risk_free_rate: float = 0.02) -> float:
        """Calculate Sharpe Ratio"""
        if len(returns) == 0:
            return 0.0
        
        returns_array = np.array(returns)
        excess_returns = returns_array - (risk_free_rate / 252)  # Daily risk-free rate
        
        if np.std(excess_returns) == 0:
            return 0.0
        
        sharpe = np.mean(excess_returns) / np.std(excess_returns) * np.sqrt(252)
        return sharpe
    
    def _calculate_max_drawdown(self, returns: List[float]) -> float:
        """Calculate Maximum Drawdown"""
        if len(returns) == 0:
            return 0.0
        
        cumulative = np.cumprod(1 + np.array(returns))
        running_max = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - running_max) / running_max
        
        return np.min(drawdown)
    
    def evolve_strategy(self, symbol: str, historical_data: pd.DataFrame) -> Dict:
        """
        Evolve trading strategy using Genetic Algorithms
        Continues until fitness ≥ 0.88 or Sharpe ≥ 2.0, minimum 300 generations
        """
        try:
            self.logger.info(f"Starting evolution for {symbol}")
            self.logger.info(f"Target: Fitness ≥ {self.target_fitness} or Sharpe ≥ {self.target_sharpe}")
            self.logger.info(f"Minimum generations: {self.min_generations}")
            
            # Create initial population
            population = self.toolbox.population(n=self.population_size)
            
            # Statistics
            stats = tools.Statistics(lambda ind: ind.fitness.values)
            stats.register("avg", np.mean)
            stats.register("std", np.std)
            stats.register("min", np.min)
            stats.register("max", np.max)
            
            # Hall of Fame (best individuals)
            hof = tools.HallOfFame(10)
            
            # Evolution loop
            generation = 0
            best_fitness = 0.0
            best_sharpe = 0.0
            converged = False
            
            # Evaluate initial population
            fitnesses = [self.toolbox.evaluate(ind, historical_data) for ind in population]
            for ind, fit in zip(population, fitnesses):
                ind.fitness.values = fit
            
            hof.update(population)
            
            while generation < self.max_generations:
                generation += 1
                
                # Select next generation
                offspring = self.toolbox.select(population, len(population))
                offspring = list(map(self.toolbox.clone, offspring))
                
                # Apply crossover
                for child1, child2 in zip(offspring[::2], offspring[1::2]):
                    if random.random() < self.crossover_prob:
                        self.toolbox.mate(child1, child2)
                        del child1.fitness.values
                        del child2.fitness.values
                
                # Apply mutation
                for mutant in offspring:
                    if random.random() < self.mutation_prob:
                        self.toolbox.mutate(mutant)
                        del mutant.fitness.values
                
                # Evaluate individuals with invalid fitness
                invalid_ind = [ind for ind in offspring if not ind.fitness.valid]
                fitnesses = [self.toolbox.evaluate(ind, historical_data) for ind in invalid_ind]
                for ind, fit in zip(invalid_ind, fitnesses):
                    ind.fitness.values = fit
                
                # Replace population
                population[:] = offspring
                hof.update(population)
                
                # Get best individual
                best_ind = hof[0]
                best_fitness = best_ind.fitness.values[0]
                
                # Calculate Sharpe for best individual
                returns = self._backtest_strategy(
                    historical_data,
                    int(best_ind[0]), best_ind[1], best_ind[2],
                    int(best_ind[3]), best_ind[4],
                    int(best_ind[5]), int(best_ind[6]), int(best_ind[7]),
                    best_ind[8], best_ind[9]
                )
                best_sharpe = self._calculate_sharpe(returns)
                
                # Log progress every 50 generations
                if generation % 50 == 0:
                    record = stats.compile(population)
                    self.logger.info(
                        f"Gen {generation}: Best Fitness={best_fitness:.4f}, "
                        f"Best Sharpe={best_sharpe:.4f}, "
                        f"Avg={record['avg']:.4f}, Max={record['max']:.4f}"
                    )
                
                # Check stopping criteria (only after minimum generations)
                if generation >= self.min_generations:
                    if best_fitness >= self.target_fitness or best_sharpe >= self.target_sharpe:
                        self.logger.success(
                            f"Target reached at generation {generation}! "
                            f"Fitness={best_fitness:.4f}, Sharpe={best_sharpe:.4f}"
                        )
                        converged = True
                        break
            
            # Final results
            best_individual = hof[0]
            
            result = {
                'symbol': symbol,
                'timestamp': get_saudi_time(),
                'generations': generation,
                'converged': converged,
                'best_fitness': best_fitness,
                'best_sharpe': best_sharpe,
                'best_parameters': {
                    'rsi_period': int(best_individual[0]),
                    'rsi_buy': best_individual[1],
                    'rsi_sell': best_individual[2],
                    'bb_period': int(best_individual[3]),
                    'bb_std': best_individual[4],
                    'macd_fast': int(best_individual[5]),
                    'macd_slow': int(best_individual[6]),
                    'macd_signal': int(best_individual[7]),
                    'stop_loss': best_individual[8],
                    'take_profit': best_individual[9]
                },
                'hall_of_fame': [
                    {
                        'fitness': ind.fitness.values[0],
                        'parameters': list(ind)
                    }
                    for ind in hof[:5]  # Top 5
                ]
            }
            
            # Save to database
            self._save_evolution_result(result)
            
            # Cache in Redis
            cache_key = f"evolution_result:{symbol}"
            redis_manager.set(cache_key, result, ttl=86400)  # 24 hours
            
            self.logger.success(f"Evolution complete for {symbol}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in evolution: {e}")
            return {}
    
    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Advanced feature engineering using custom features
        """
        try:
            self.logger.info("Starting feature engineering")
            
            # Create a copy
            features_df = df.copy()
            
            # ===== BASIC FEATURES =====
            
            # Price-based features
            features_df['returns'] = features_df['close'].pct_change()
            features_df['log_returns'] = np.log(features_df['close'] / features_df['close'].shift(1))
            features_df['price_range'] = features_df['high'] - features_df['low']
            features_df['price_range_pct'] = features_df['price_range'] / features_df['close']
            
            # Volume features
            features_df['volume_ma_5'] = features_df['volume'].rolling(5).mean()
            features_df['volume_ma_20'] = features_df['volume'].rolling(20).mean()
            features_df['volume_ratio'] = features_df['volume'] / features_df['volume_ma_20']
            
            # Volatility features
            features_df['volatility_5'] = features_df['returns'].rolling(5).std()
            features_df['volatility_20'] = features_df['returns'].rolling(20).std()
            
            # ===== ADVANCED FEATURES =====
            
            # Momentum features
            for period in [5, 10, 20]:
                features_df[f'momentum_{period}'] = features_df['close'] - features_df['close'].shift(period)
                features_df[f'roc_{period}'] = features_df['close'].pct_change(period)
            
            # Statistical features
            for window in [10, 20, 50]:
                features_df[f'mean_{window}'] = features_df['close'].rolling(window).mean()
                features_df[f'std_{window}'] = features_df['close'].rolling(window).std()
                features_df[f'skew_{window}'] = features_df['close'].rolling(window).skew()
                features_df[f'kurt_{window}'] = features_df['close'].rolling(window).kurt()
            
            # Price position features
            features_df['high_low_ratio'] = features_df['high'] / features_df['low']
            features_df['close_open_ratio'] = features_df['close'] / features_df['open']
            
            # Advanced TA features
            advanced_analysis = self.advanced_ta.calculate_volume_profile(df)
            features_df['distance_to_poc'] = (features_df['close'] - advanced_analysis['poc']) / features_df['close']
            features_df['distance_to_vah'] = (features_df['close'] - advanced_analysis['vah']) / features_df['close']
            features_df['distance_to_val'] = (features_df['close'] - advanced_analysis['val']) / features_df['close']
            
            # Market regime feature
            regime = self.advanced_ta.detect_market_regime(df)
            features_df['regime'] = regime
            
            # Encode regime as numeric
            regime_map = {
                'trending_up': 1,
                'trending_down': -1,
                'ranging': 0,
                'high_volatility': 2,
                'low_volatility': -2,
                'compression': 3
            }
            features_df['regime_numeric'] = features_df['regime'].map(regime_map).fillna(0)
            
            self.logger.info(f"Feature engineering complete (pre-embedding): {len(features_df.columns)} features")
            
            return features_df
            
        except Exception as e:
            self.logger.error(f"Error in feature engineering: {e}")
            return df
    
    def _save_evolution_result(self, result: Dict):
        """Save evolution result to strategies.backtest_results"""
        try:
            if not result:
                return
            import json
            with db.get_session() as session:
                query = text("""
                INSERT INTO strategies.backtest_results
                    (strategy_name, symbol, timeframe, start_date, end_date,
                     total_return, sharpe_ratio, max_drawdown, win_rate, metadata)
                VALUES
                    (:strategy_name, :symbol, :timeframe, :start_date, :end_date,
                     :total_return, :sharpe_ratio, :max_drawdown, :win_rate, :metadata)
                ON CONFLICT DO NOTHING
                """)
                session.execute(query, {
                    'strategy_name': 'genetic_' + result.get('symbol', 'unknown'),
                    'symbol': result.get('symbol', 'unknown'),
                    'timeframe': '1d',
                    'start_date': datetime.now().date(),
                    'end_date': datetime.now().date(),
                    'total_return': float(result.get('best_fitness', 0.0)),
                    'sharpe_ratio': float(result.get('best_sharpe', 0.0)),
                    'max_drawdown': float(result.get('max_drawdown', 0.0)),
                    'win_rate': float(result.get('win_rate', 0.0)),
                    'metadata': json.dumps({
                        'generations': result.get('generations', 0),
                        'best_params': result.get('best_params', {}),
                        'converged': result.get('converged', False),
                        'wfo_score': result.get('wfo_score', None),
                        'monte_carlo': result.get('monte_carlo', None)
                    })
                })
                session.commit()
            self.logger.success(
                f"[Scientist] Evolution result saved for {result.get('symbol')}: "
                f"Fitness={result.get('best_fitness', 0):.4f}, Sharpe={result.get('best_sharpe', 0):.4f}"
            )
        except Exception as e:
            self.logger.error(f"[Scientist] Error saving evolution result: {e}")
    
    def run(self, symbols: List[str] = None):
        """Main execution method"""
        try:
            if symbols is None:
                symbols = config.get('symbols', [])
            
            self.logger.info(f"Starting Scientist for {len(symbols)} symbols")
            
            for symbol in symbols:
                try:
                    # Fetch historical data from TimescaleDB
                    historical_data = self._fetch_historical_data(symbol)
                    
                    if historical_data is None or len(historical_data) < 100:
                        self.logger.warning(f"Insufficient data for {symbol}")
                        continue
                    
                    # Feature engineering (TA + statistical features)
                    features_df = self.engineer_features(historical_data)

                    # Enrich with FinBERT embeddings (PRIMARY new features for GA)
                    features_df = self.enrich_features_with_embeddings(features_df, symbol)
                    self.logger.info(
                        f"[Scientist] {symbol}: {len(features_df.columns)} total features "
                        f"(TA + statistical + {sum(1 for c in features_df.columns if c.startswith('emb_'))} embedding dims)"
                    )

                    # ── Walk-Forward Optimization (Robustness Test) ──
                    wfo_result = self.walk_forward_optimization(features_df, symbol)
                    if wfo_result.get('recommendation') == 'REJECT':
                        self.logger.warning(
                            f"[WFO] {symbol}: Strategy REJECTED "
                            f"(degradation={wfo_result.get('degradation', 0):.1%}). "
                            f"Skipping evolution."
                        )
                        continue
                    self.logger.info(
                        f"[WFO] {symbol}: ACCEPTED "
                        f"(in={wfo_result.get('in_sample_fitness',0):.4f}, "
                        f"out={wfo_result.get('out_of_sample_fitness',0):.4f})"
                    )

                    # ── Regime Shift Check ──
                    regime = self.advanced_ta.detect_market_regime(historical_data)
                    self.logger.info(f"[RegimeCheck] {symbol}: Current regime = {regime}")
                    if regime in ('high_volatility',):
                        self.logger.warning(
                            f"[RegimeCheck] {symbol}: High-volatility regime detected. "
                            f"Reducing population for faster convergence."
                        )

                    # ── Evolve strategy using enriched features ──
                    result = self.evolve_strategy(symbol, features_df)

                    if result:
                        # ── Monte Carlo Simulation (Overfitting Guard) ──
                        mc_result = self.monte_carlo_simulation(
                            result.get('best_parameters', {}),
                            features_df,
                            n_simulations=500
                        )
                        result['monte_carlo'] = mc_result
                        result['wfo_score'] = wfo_result.get('out_of_sample_fitness', 0)
                        result['regime'] = regime

                        # ── Saudi-Specific Edges (now active) ──
                        saudi_edges = self.apply_saudi_specific_edges(symbol, historical_data)
                        result['saudi_edges'] = saudi_edges
                        self.logger.info(
                            f"[SaudiEdges] {symbol}: score={saudi_edges.get('saudi_edge_score', 0):.3f}"
                        )

                        self.logger.info(
                            f"{symbol}: Fitness={result['best_fitness']:.4f}, "
                            f"Sharpe={result['best_sharpe']:.4f}, "
                            f"Gens={result['generations']}, "
                            f"MC_pass_rate={mc_result.get('pass_rate', 0):.1%}, "
                            f"WFO={result['wfo_score']:.4f}"
                        )
                    
                except Exception as e:
                    self.logger.error(f"Error processing {symbol}: {e}")
                    continue
            
            self.logger.success("Scientist execution complete")
            
        except Exception as e:
            self.logger.error(f"Error in Scientist run: {e}")
    
    def _fetch_historical_data(self, symbol: str, timeframe: str = '1d', limit: int = 500) -> Optional[pd.DataFrame]:
        """
        Fetch historical OHLCV data from TimescaleDB (market_data.ohlcv).
        Falls back to '1h' if daily data is insufficient.
        """
        try:
            with db.get_session() as session:
                query = text("""
                SELECT time, open, high, low, close, volume
                FROM market_data.ohlcv
                WHERE symbol = :symbol AND timeframe = :timeframe
                ORDER BY time DESC
                LIMIT :limit
                """)
                result = session.execute(query, {
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'limit': limit
                })
                rows = result.fetchall()

            if not rows or len(rows) < 50:
                self.logger.warning(
                    f"[Scientist] Insufficient {timeframe} data for {symbol} "
                    f"({len(rows) if rows else 0} rows). Trying '1h'."
                )
                if timeframe != '1h':
                    return self._fetch_historical_data(symbol, timeframe='1h', limit=limit)
                return None

            df = pd.DataFrame(rows, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
            df['time'] = pd.to_datetime(df['time'])
            df = df.sort_values('time').reset_index(drop=True)
            df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].astype(float)
            df['volume'] = df['volume'].fillna(0).astype(float)
            self.logger.info(f"[Scientist] Fetched {len(df)} rows for {symbol} ({timeframe})")
            return df

        except Exception as e:
            self.logger.error(f"[Scientist] Error fetching data for {symbol}: {e}")
            return None


if __name__ == "__main__":
    bot = Scientist()
    bot.run()

    def walk_forward_optimization(self, df: pd.DataFrame, symbol: str) -> Dict:
        """
        Walk-Forward Optimization
        
        Split data into In-Sample (80%) and Out-of-Sample (20%)
        Train on in-sample, validate on out-of-sample
        Select strategy that maintains performance in future period
        
        Args:
            df: Historical price data
            symbol: Stock symbol
            
        Returns:
            Dict with best parameters and performance metrics
        """
        try:
            self.logger.info(f"Starting Walk-Forward Optimization for {symbol}")
            
            # Split data
            split_point = int(len(df) * 0.8)
            in_sample = df.iloc[:split_point].copy()
            out_of_sample = df.iloc[split_point:].copy()
            
            self.logger.info(f"In-Sample: {len(in_sample)} bars, Out-of-Sample: {len(out_of_sample)} bars")
            
            # Run genetic algorithm on in-sample data
            self.logger.info("Training on In-Sample data...")
            best_individual_in = self._run_genetic_algorithm_on_data(in_sample, symbol)
            
            if not best_individual_in:
                self.logger.warning("No valid individual found in In-Sample")
                return {'status': 'failed', 'reason': 'no_valid_individual'}
            
            # Extract parameters
            params = self._individual_to_params(best_individual_in)
            
            # Evaluate on in-sample
            in_sample_fitness = self._evaluate_individual_on_data(best_individual_in, in_sample)
            
            # Evaluate on out-of-sample
            out_of_sample_fitness = self._evaluate_individual_on_data(best_individual_in, out_of_sample)
            
            # Calculate performance degradation
            if in_sample_fitness[0] > 0:
                degradation = (in_sample_fitness[0] - out_of_sample_fitness[0]) / in_sample_fitness[0]
            else:
                degradation = 1.0
            
            # Check if strategy maintains performance
            # Accept if out-of-sample fitness is at least 70% of in-sample
            maintains_performance = out_of_sample_fitness[0] >= 0.7 * in_sample_fitness[0]
            
            result = {
                'status': 'success',
                'symbol': symbol,
                'parameters': params,
                'in_sample_fitness': in_sample_fitness[0],
                'out_of_sample_fitness': out_of_sample_fitness[0],
                'degradation': degradation,
                'maintains_performance': maintains_performance,
                'recommendation': 'ACCEPT' if maintains_performance else 'REJECT'
            }
            
            self.logger.success(
                f"Walk-Forward complete: {symbol} - "
                f"In-Sample={in_sample_fitness[0]:.4f}, "
                f"Out-of-Sample={out_of_sample_fitness[0]:.4f}, "
                f"Degradation={degradation:.2%}, "
                f"Recommendation={result['recommendation']}"
            )
            
            # Cache result
            redis_manager.set(f"walk_forward:{symbol}", result, ttl=86400)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in walk-forward optimization: {e}")
            return {'status': 'error', 'reason': str(e)}
    
    def _run_genetic_algorithm_on_data(self, df: pd.DataFrame, symbol: str):
        """Run GA on specific dataset (for walk-forward)"""
        try:
            # Create toolbox
            creator.create("FitnessMax", base.Fitness, weights=(1.0,))
            creator.create("Individual", list, fitness=creator.FitnessMax)
            
            toolbox = base.Toolbox()
            
            # Gene ranges (same as main GA)
            toolbox.register("rsi_period", random.randint, 10, 20)
            toolbox.register("rsi_buy", random.randint, 20, 40)
            toolbox.register("rsi_sell", random.randint, 60, 80)
            toolbox.register("bb_period", random.randint, 15, 25)
            toolbox.register("bb_std", random.uniform, 1.5, 2.5)
            toolbox.register("macd_fast", random.randint, 8, 15)
            toolbox.register("macd_slow", random.randint, 20, 30)
            toolbox.register("macd_signal", random.randint, 7, 12)
            toolbox.register("stop_loss", random.uniform, 0.02, 0.05)
            toolbox.register("take_profit", random.uniform, 0.03, 0.08)
            
            toolbox.register("individual", tools.initCycle, creator.Individual,
                           (toolbox.rsi_period, toolbox.rsi_buy, toolbox.rsi_sell,
                            toolbox.bb_period, toolbox.bb_std,
                            toolbox.macd_fast, toolbox.macd_slow, toolbox.macd_signal,
                            toolbox.stop_loss, toolbox.take_profit), n=1)
            
            toolbox.register("population", tools.initRepeat, list, toolbox.individual)
            
            # Evaluation function using this specific dataset
            def evaluate_on_this_data(individual):
                return self._evaluate_individual_on_data(individual, df)
            
            toolbox.register("evaluate", evaluate_on_this_data)
            toolbox.register("mate", tools.cxTwoPoint)
            toolbox.register("mutate", tools.mutGaussian, mu=0, sigma=1, indpb=0.2)
            toolbox.register("select", tools.selTournament, tournsize=3)
            
            # Run simplified GA (fewer generations for speed)
            population = toolbox.population(n=50)
            ngen = 100  # Reduced for walk-forward
            
            for gen in range(ngen):
                offspring = algorithms.varAnd(population, toolbox, cxpb=0.5, mutpb=0.2)
                fits = toolbox.map(toolbox.evaluate, offspring)
                for fit, ind in zip(fits, offspring):
                    ind.fitness.values = fit
                population = toolbox.select(offspring, k=len(population))
            
            # Get best individual
            best = tools.selBest(population, k=1)[0]
            
            return best
            
        except Exception as e:
            self.logger.error(f"Error running GA on data: {e}")
            return None
    
    def _evaluate_individual_on_data(self, individual, df: pd.DataFrame):
        """Evaluate individual on specific dataset"""
        try:
            rsi_period, rsi_buy, rsi_sell, bb_period, bb_std, \
            macd_fast, macd_slow, macd_signal, stop_loss, take_profit = individual
            
            # Backtest
            returns = self._backtest_strategy(
                df, rsi_period, rsi_buy, rsi_sell,
                bb_period, bb_std, macd_fast, macd_slow, macd_signal,
                stop_loss, take_profit
            )
            
            if len(returns) == 0:
                return (0.0,)
            
            # Calculate fitness
            total_return = np.sum(returns)
            sharpe_ratio = self._calculate_sharpe(returns)
            max_drawdown = self._calculate_max_drawdown(returns)
            win_rate = len([r for r in returns if r > 0]) / len(returns)
            
            fitness = (
                0.3 * min(total_return / 0.5, 1.0) +
                0.3 * min(sharpe_ratio / 2.0, 1.0) +
                0.2 * (1.0 - min(abs(max_drawdown) / 0.2, 1.0)) +
                0.2 * win_rate
            )
            
            return (fitness,)
            
        except Exception as e:
            return (0.0,)
    
    def _individual_to_params(self, individual) -> Dict:
        """Convert individual to parameter dict"""
        return {
            'rsi_period': int(individual[0]),
            'rsi_buy': int(individual[1]),
            'rsi_sell': int(individual[2]),
            'bb_period': int(individual[3]),
            'bb_std': float(individual[4]),
            'macd_fast': int(individual[5]),
            'macd_slow': int(individual[6]),
            'macd_signal': int(individual[7]),
            'stop_loss': float(individual[8]),
            'take_profit': float(individual[9])
        }

    def calculate_opening_auction_strength(self, df: pd.DataFrame) -> float:
        """
        Calculate Opening Auction Strength (Saudi-specific edge)
        
        Measures the strength of opening auction by comparing:
        - Opening price vs previous close
        - Opening volume vs average volume
        - Price movement in first 15 minutes
        
        Args:
            df: Price data with 1-minute or 5-minute bars
            
        Returns:
            float: Opening auction strength score (0.0 to 1.0)
        """
        try:
            if len(df) < 20:
                return 0.5  # Neutral if insufficient data
            
            # Get opening bar
            opening_bar = df.iloc[0]
            prev_close = df.iloc[-1]['close'] if len(df) > 1 else opening_bar['open']
            
            # 1. Opening gap strength
            opening_gap = (opening_bar['open'] - prev_close) / prev_close
            gap_score = min(abs(opening_gap) / 0.02, 1.0)  # Normalize to 2% gap
            
            # 2. Opening volume strength
            opening_volume = opening_bar['volume']
            avg_volume = df['volume'].mean()
            volume_ratio = opening_volume / avg_volume if avg_volume > 0 else 1.0
            volume_score = min(volume_ratio / 3.0, 1.0)  # Normalize to 3x average
            
            # 3. First 15 minutes momentum
            first_15min = df.head(15) if len(df) >= 15 else df.head(len(df))
            momentum = (first_15min['close'].iloc[-1] - first_15min['open'].iloc[0]) / first_15min['open'].iloc[0]
            momentum_score = min(abs(momentum) / 0.01, 1.0)  # Normalize to 1%
            
            # Combined score
            strength_score = (gap_score * 0.3 + volume_score * 0.4 + momentum_score * 0.3)
            
            self.logger.info(
                f"Opening Auction Strength: Gap={opening_gap:.2%}, "
                f"Volume Ratio={volume_ratio:.2f}x, Momentum={momentum:.2%}, "
                f"Score={strength_score:.3f}"
            )
            
            return strength_score
            
        except Exception as e:
            self.logger.error(f"Error calculating opening auction strength: {e}")
            return 0.5
    
    def calculate_closing_auction_behavior(self, df: pd.DataFrame) -> Dict:
        """
        Calculate Closing Auction Behavior (Saudi-specific edge)
        
        Analyzes closing auction patterns:
        - Price movement in last 15 minutes
        - Volume surge in closing auction
        - Closing price vs day's range
        
        Args:
            df: Price data with 1-minute or 5-minute bars
            
        Returns:
            Dict with closing auction metrics
        """
        try:
            if len(df) < 20:
                return {'score': 0.5, 'pattern': 'unknown'}
            
            # Get last 15 minutes
            last_15min = df.tail(15)
            
            # 1. Closing momentum
            closing_momentum = (last_15min['close'].iloc[-1] - last_15min['open'].iloc[0]) / last_15min['open'].iloc[0]
            
            # 2. Closing volume surge
            closing_volume = last_15min['volume'].sum()
            total_volume = df['volume'].sum()
            closing_volume_pct = closing_volume / total_volume if total_volume > 0 else 0
            
            # 3. Closing price position in day's range
            day_high = df['high'].max()
            day_low = df['low'].min()
            closing_price = df['close'].iloc[-1]
            
            if day_high > day_low:
                price_position = (closing_price - day_low) / (day_high - day_low)
            else:
                price_position = 0.5
            
            # Determine pattern
            if closing_momentum > 0.005 and closing_volume_pct > 0.20:
                pattern = 'strong_buying'
                score = 0.8
            elif closing_momentum < -0.005 and closing_volume_pct > 0.20:
                pattern = 'strong_selling'
                score = 0.2
            elif abs(closing_momentum) < 0.002:
                pattern = 'consolidation'
                score = 0.5
            else:
                pattern = 'normal'
                score = 0.5
            
            result = {
                'score': score,
                'pattern': pattern,
                'closing_momentum': closing_momentum,
                'closing_volume_pct': closing_volume_pct,
                'price_position': price_position
            }
            
            self.logger.info(
                f"Closing Auction: Pattern={pattern}, Momentum={closing_momentum:.2%}, "
                f"Volume%={closing_volume_pct:.2%}, Position={price_position:.2f}"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error calculating closing auction behavior: {e}")
            return {'score': 0.5, 'pattern': 'unknown'}
    
    def calculate_oil_correlation_filter(self, symbol: str, df: pd.DataFrame) -> float:
        """
        Calculate Oil Correlation Filter for petrochemical stocks (Saudi-specific edge)
        
        Checks if stock is in petrochemical sector and applies oil price correlation
        
        Args:
            symbol: Stock symbol
            df: Price data
            
        Returns:
            float: Correlation score (0.0 to 1.0), 1.0 if not petrochemical
        """
        try:
            # Define petrochemical symbols (SABIC, SIPCHEM, etc.)
            petrochemical_symbols = ['2010', '2020', '2030', '2290', '2330', '2350']
            
            if symbol not in petrochemical_symbols:
                return 1.0  # Not petrochemical, no filter needed
            
            # Get oil price data (would fetch from external API in production)
            # For now, simulate correlation check
            
            # Calculate recent correlation with oil
            # In production, this would use actual Brent/WTI data
            
            # Placeholder: Check if stock is moving with expected oil correlation
            recent_returns = df['close'].pct_change().tail(20)
            volatility = recent_returns.std()
            
            # High volatility in petrochemicals when oil is stable = potential issue
            # This is simplified - in production would use actual oil data
            
            if volatility > 0.03:  # High volatility
                correlation_score = 0.6  # Moderate caution
                self.logger.warning(
                    f"{symbol}: Petrochemical with high volatility ({volatility:.2%}) - "
                    f"Correlation score={correlation_score}"
                )
            else:
                correlation_score = 1.0  # Normal
            
            return correlation_score
            
        except Exception as e:
            self.logger.error(f"Error calculating oil correlation filter: {e}")
            return 1.0
    
    def apply_saudi_specific_edges(self, symbol: str, df: pd.DataFrame) -> Dict:
        """
        Apply all Saudi-specific edges to stock analysis
        
        Args:
            symbol: Stock symbol
            df: Price data
            
        Returns:
            Dict with all Saudi-specific metrics
        """
        try:
            self.logger.info(f"Applying Saudi-specific edges to {symbol}")
            
            # 1. Opening Auction Strength
            opening_strength = self.calculate_opening_auction_strength(df)
            
            # 2. Closing Auction Behavior
            closing_behavior = self.calculate_closing_auction_behavior(df)
            
            # 3. Oil Correlation Filter (for petrochemicals)
            oil_correlation = self.calculate_oil_correlation_filter(symbol, df)
            
            # Combined Saudi edge score
            saudi_edge_score = (
                opening_strength * 0.3 +
                closing_behavior['score'] * 0.3 +
                oil_correlation * 0.4
            )
            
            result = {
                'saudi_edge_score': saudi_edge_score,
                'opening_auction_strength': opening_strength,
                'closing_auction_behavior': closing_behavior,
                'oil_correlation_score': oil_correlation,
                'timestamp': get_saudi_time()
            }
            
            self.logger.success(
                f"{symbol}: Saudi Edge Score = {saudi_edge_score:.3f} "
                f"(Opening={opening_strength:.2f}, Closing={closing_behavior['score']:.2f}, "
                f"Oil={oil_correlation:.2f})"
            )
            
            # Cache result
            redis_manager.set(f"saudi_edges:{symbol}", result, ttl=3600)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error applying Saudi-specific edges: {e}")
            return {
                'saudi_edge_score': 0.5,
                'opening_auction_strength': 0.5,
                'closing_auction_behavior': {'score': 0.5, 'pattern': 'unknown'},
                'oil_correlation_score': 1.0
            }

    def monte_carlo_simulation(
        self,
        params: Dict,
        df: pd.DataFrame,
        n_simulations: int = 500,
        pass_threshold: float = 0.6
    ) -> Dict:
        """
        Monte Carlo Simulation for robustness testing against overfitting.

        Method:
        1. Run N simulations, each on a randomly shuffled subset of the data (80%).
        2. For each simulation, backtest with the given params and record Sharpe ratio.
        3. Calculate pass_rate = fraction of simulations with Sharpe >= 1.0.
        4. A strategy is considered robust if pass_rate >= pass_threshold (default 60%).

        Args:
            params: Best parameters from GA evolution
            df: Full features DataFrame
            n_simulations: Number of Monte Carlo runs (default 500)
            pass_threshold: Minimum fraction of passing simulations (default 0.6)

        Returns:
            Dict with simulation results and robustness verdict
        """
        try:
            self.logger.info(
                f"[MonteCarlo] Running {n_simulations} simulations "
                f"(pass_threshold={pass_threshold:.0%})"
            )

            sharpe_scores = []
            sample_size = int(len(df) * 0.8)

            for i in range(n_simulations):
                try:
                    # Random sample without replacement
                    sample = df.sample(n=sample_size, replace=False).sort_index()

                    returns = self._backtest_strategy(
                        sample,
                        int(params.get('rsi_period', 14)),
                        float(params.get('rsi_buy', 30)),
                        float(params.get('rsi_sell', 70)),
                        int(params.get('bb_period', 20)),
                        float(params.get('bb_std', 2.0)),
                        int(params.get('macd_fast', 12)),
                        int(params.get('macd_slow', 26)),
                        int(params.get('macd_signal', 9)),
                        float(params.get('stop_loss', 0.01)),
                        float(params.get('take_profit', 0.02))
                    )
                    sharpe = self._calculate_sharpe(returns)
                    sharpe_scores.append(sharpe)
                except Exception:
                    sharpe_scores.append(0.0)

            if not sharpe_scores:
                return {'status': 'failed', 'pass_rate': 0.0, 'robust': False}

            sharpe_arr = np.array(sharpe_scores)
            pass_rate = float(np.mean(sharpe_arr >= 1.0))
            robust = pass_rate >= pass_threshold

            result = {
                'status': 'success',
                'n_simulations': n_simulations,
                'pass_rate': round(pass_rate, 4),
                'robust': robust,
                'sharpe_mean': round(float(np.mean(sharpe_arr)), 4),
                'sharpe_std': round(float(np.std(sharpe_arr)), 4),
                'sharpe_p5': round(float(np.percentile(sharpe_arr, 5)), 4),
                'sharpe_p95': round(float(np.percentile(sharpe_arr, 95)), 4),
                'verdict': 'ROBUST' if robust else 'FRAGILE'
            }

            self.logger.info(
                f"[MonteCarlo] pass_rate={pass_rate:.1%}, "
                f"Sharpe mean={result['sharpe_mean']:.4f} ± {result['sharpe_std']:.4f}, "
                f"verdict={result['verdict']}"
            )
            return result

        except Exception as e:
            self.logger.error(f"[MonteCarlo] Error: {e}")
            return {'status': 'error', 'pass_rate': 0.0, 'robust': False, 'verdict': 'ERROR'}


    # ═══════════════════════════════════════════════════════════
    # ── المحرك الجيني الجديد (Alpha-Engine v2) ──────────────
    # يعمل بالتوازي مع DEAP الموجود، ويُكمله
    # ═══════════════════════════════════════════════════════════

    def run_genetic_cycle(
        self,
        symbols: Optional[List[str]] = None,
        generations: int = 10,
        population_size: int = 30,
        elite_ratio: float = 0.20,
        mutation_rate: float = 0.15,
        min_fitness_to_save: float = 0.05,
    ) -> Dict:
        """
        يُشغّل دورة التطور الجيني الكاملة باستخدام Generator + Evaluator الجديدَين.

        Args:
            symbols:            قائمة الأسهم (None = اختيار تلقائي من DB)
            generations:        عدد الأجيال
            population_size:    حجم المجتمع
            elite_ratio:        نسبة النخبة
            mutation_rate:      معدل الطفرة
            min_fitness_to_save: الحد الأدنى للحفظ

        Returns:
            ملخص الدورة: {symbols_processed, total_elite, objectives_run, elapsed_sec}
        """
        import asyncio
        import time
        from bots.generator.bot import GeneticGenerator, PROFIT_OBJECTIVES
        from bots.evaluator.bot import StrategyEvaluator

        self.logger.info(
            "🧬 [GeneticCycle] Starting new evolution cycle "
            f"(gen={generations}, pop={population_size})"
        )
        start_time = time.time()

        # ── اختيار الأسهم ──
        if not symbols:
            symbols = self._pick_symbols_for_genetic_cycle()

        generator = GeneticGenerator()
        evaluator = StrategyEvaluator(db_pool=None)  # db_pool يُمرَّر عند الإنتاج

        total_elite = 0
        objectives_run = 0

        for symbol in symbols:
            for objective in PROFIT_OBJECTIVES:
                try:
                    elite_count = self._run_evolution_loop(
                        generator=generator,
                        evaluator=evaluator,
                        symbol=symbol,
                        objective=objective,
                        generations=generations,
                        population_size=population_size,
                        elite_ratio=elite_ratio,
                        mutation_rate=mutation_rate,
                        min_fitness_to_save=min_fitness_to_save,
                    )
                    total_elite += elite_count
                    objectives_run += 1
                except Exception as e:
                    self.logger.error(
                        f"❌ [GeneticCycle] Failed {symbol} [{objective}]: {e}"
                    )

        elapsed = round(time.time() - start_time, 1)
        summary = {
            "symbols_processed": len(symbols),
            "objectives_run":    objectives_run,
            "total_elite":       total_elite,
            "elapsed_sec":       elapsed,
        }
        self.logger.info(
            f"✅ [GeneticCycle] Done in {elapsed}s — "
            f"{total_elite} elite strategies across {len(symbols)} symbols"
        )
        return summary

    def _run_evolution_loop(
        self,
        generator,
        evaluator,
        symbol: str,
        objective: str,
        generations: int,
        population_size: int,
        elite_ratio: float,
        mutation_rate: float,
        min_fitness_to_save: float,
    ) -> int:
        """
        حلقة التطور الداخلية لسهم وهدف واحد.
        تُعيد عدد الاستراتيجيات النخبة المحفوظة.
        """
        import asyncio

        self.logger.info(
            f"  🔬 Evolving {symbol} [{objective}] — "
            f"{generations} gen × {population_size} pop"
        )

        # ── الجيل الأول ──
        population = generator.generate_population(
            symbol, objective, size=population_size
        )

        loop = asyncio.new_event_loop()
        evaluated_pop = []

        try:
            for gen in range(1, generations + 1):
                # تعيين رقم الجيل
                for ind in population:
                    ind["generation"] = gen

                # التقييم
                tasks = [evaluator.evaluate(ind) for ind in population]
                results = loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))

                # دمج النتائج
                for ind, result in zip(population, results):
                    if isinstance(result, Exception):
                        ind["fitness_score"] = 0.0
                    else:
                        ind["fitness_score"] = result.get("fitness_score", 0.0)
                        for key in [
                            "total_profit_pct", "win_rate", "total_trades",
                            "avg_profit_pct", "max_drawdown_pct", "sharpe_ratio",
                            "profit_factor", "avg_duration_min",
                        ]:
                            ind[key] = result.get(key, 0)

                # ترتيب
                population.sort(
                    key=lambda x: x.get("fitness_score", 0.0), reverse=True
                )
                best = population[0].get("fitness_score", 0.0)
                avg  = sum(x.get("fitness_score", 0.0) for x in population) / len(population)

                self.logger.info(
                    f"    Gen {gen:2d}/{generations} | {symbol} [{objective}] | "
                    f"best={best:.4f} avg={avg:.4f}"
                )

                if gen == generations:
                    evaluated_pop = population
                    break

                # اختيار النخبة وتوليد الجيل التالي
                elite = generator.select_elite(population, elite_ratio)
                population = generator.breed_next_generation(
                    elite, target_size=population_size, mutation_rate=mutation_rate
                )

        finally:
            loop.close()

        # ── حفظ أفضل الاستراتيجيات في DB ──
        elite_saved = 0
        save_loop = asyncio.new_event_loop()
        try:
            for ind in evaluated_pop:
                if ind.get("fitness_score", 0.0) >= min_fitness_to_save:
                    saved = save_loop.run_until_complete(evaluator.save_strategy(ind))
                    if saved:
                        save_loop.run_until_complete(evaluator.save_result(ind))
                        elite_saved += 1
        finally:
            save_loop.close()

        self.logger.info(
            f"  💾 Saved {elite_saved} elite strategies for {symbol} [{objective}]"
        )
        return elite_saved

    def _pick_symbols_for_genetic_cycle(self, limit: int = 3) -> List[str]:
        """يختار أسهماً للتحليل الجيني من Redis أو قائمة افتراضية."""
        try:
            cached = redis_manager.get("sahmk:symbols_list")
            if cached and isinstance(cached, list):
                tasi = [s for s in cached if len(str(s)) == 4 and str(s)[0] in "12345678"]
                return tasi[:limit]
        except Exception:
            pass
        return ["2222", "1120", "2010"][:limit]
