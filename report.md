# End-Term Report: Sentinel-Stream (Real-Time Investment Decision & Alerts Engine)

Recent platforms like QuantConnect, Alpaca, and Bloomberg Terminal have started using machine learning and NLP for trading insights, with frameworks like FinRL and models like ARMA, GARCH, and XGBoost becoming industry standards for predictions. However, these systems still face major challenges with real-time processing, scalability, and explainability—they mostly run in batch mode, which works for long-term predictions but fails to react quickly to rapid market movements. That's exactly why we chose Pathway: it enables us to build a truly real-time, efficient, and intelligent system that processes data as it arrives, computes incremental updates without reprocessing everything, and delivers insights fast enough to actually matter in real-world trading scenarios. *(See Appendix 1.1)*

## Solution Overview

Our system takes a user's portfolio, predicts future price movements for their stocks, and sends alerts with buy/sell recommendations whenever significant changes are expected. It also incorporates real-time news data to improve predictions. Streaming is critical here because users need to act quickly to maximize gains and minimize losses—batch processing would make our tips too late to be useful. Pathway handles the heavy lifting through incremental updates on new data, pre-computing deterministic calculations, and providing persistence so we can recover from system failures without losing progress.

The system combines deterministic components like technical analysis (RSI, MACD, etc.), time series forecasters (ARMA, GARCH), and an XGBoost ML model to predict trends. The pipeline works in stages:
1. **Stream real-time data:** We stream real-time stock prices and news from APIs.
2. **Calculate metrics:** We calculate technical metrics and apply time series models.
3. **Machine Learning Inference:** We feed everything into XGBoost to predict price changes.
4. **Generate Alerts:** Finally, we generate actionable buy/sell alerts whenever significant movements are anticipated. *(See Appendix 1.2)*

## System Architecture

**Architecture Diagram:** [Excalidraw Link](https://excalidraw.com/#json=Kn1KmAxPWR0aybp53kacL,fTkFBmzQ8Y3vxqd05O9Miw)

1. **Ingestion of data:** We continuously ingest data of the 240 most popular and traded stocks of the US market into our backend through Kafka. Pathway consumes these Kafka topics through its Kafka connector. We also ingest real-time news related to these companies, as well as their sentiment and relevance score, through an API into Kafka.
2. **Technical and Sentiment Analysis:** For each of the stocks present in our list, Pathway calculates the technical metrics *(present in Appendix 1.5)*. The metrics are computed as incremental transformations on Pathway streaming tables for low latency. Pathway's key capabilities—live indexing, incremental updates, materialized state tables with persistence, and low-latency joins and windowing—enable instant signal updates as new data arrives, ensuring sub-second response times that eliminate costly batch solutions used by competitors with Spark or Flink. These metrics help decide when to buy or sell based on momentum and trend alignment. Pathway also processes the news stream and stores it in ClickHouse, contributing to trading decisions.
3. **Models used:** We use ARMA models to forecast future values and GARCH models to predict volatility for each stock, computed continuously over sliding windows to make instant real-time predictions. To capture non-linearity and handle numerous features, we employ two XGBoost models trained on historical price data and technical metrics across all stocks. The first predicts percentage price change, while the second identifies significant moves (changes > 2%). Our key novelty is combining streaming technical indicators, sentiment analysis, and incremental ML inference in a single real-time pipeline. All computation pipelines run continuously in the backend, minimizing latency when a user requests advice.
4. **Data Handling:** Once all the metrics are calculated, they are pushed to ClickHouse in one go. ClickHouse stores the historical data which will be used to retrain ARMA, GARCH and XGBoost on the past data.
5. **User Interaction and Recommendation Layer:** Once the user has entered their current portfolio and investment preferences, our system sends alerts to the user's mobile whenever a big change in the stock price is expected. The computed metrics and news of that particular time are packaged into an actionable alert. The alert includes the predicted price change, the current sentiment score, and key technical indicators to help the user make an informed decision.

**Pathway uniquely enables us:** Incremental streaming computation (no recomputation, ultra-low latency), stateful joins (unifies various tables in real-time) and automatic incremental materialization.

## Results and Metrics

### XGBoost Regressor
- **MSE:** 0.2007 
- **R² Score:** 0.4571

### XGBoost Classifier
- **Accuracy:** 0.9054

| Class | Precision | Recall | F1-Score | Support |
|---|---|---|---|---|
| **0** | 0.95 | 0.94 | 0.95 | 15,851 |
| **1** | 0.51 | 0.58 | 0.54 | 1,711 |
| **Accuracy** | | | **0.91** | **17,562** |
| **Macro Avg** | 0.73 | 0.76 | 0.75 | 17,562 |
| **Weighted Avg** | 0.91 | 0.91 | 0.91 | 17,562 |

**Confusion Matrix:**
```text
[[14908   943]
 [  719   992]]
```

## Implementation Details & Production Bar

### Streaming Ingest & Live Indexing
We built our system to handle real-time financial data with low latency while staying reliable. Stock ticks and news continuously flow through lightweight producers into Kafka topics, where Pathway's connectors convert them into dynamic tables. Every new tick triggers incremental updates to technical indicators, sentiment scores, ARMA/GARCH forecasts, and XGBoost feature vectors—all in under a second without recalculating entire windows. We then batch computed features periodically and write them to ClickHouse, which serves as both our historical store and ML training source, with its columnar design enabling fast reads/writes and efficient backtesting.

### Resilience & Observability
We ensure expensive operations like model inference only run after basic computations are validated, reducing cascading failures. For monitoring, we implemented structured logging through Python and Pathway to track ingestion status, latency, and failures. Before deployment, we validate everything through historical data simulations, schema validation on Kafka payloads to catch malformed data, and safety checks that convert low-confidence signals to "Hold" recommendations. *(See Appendix 1.4)*

## Challenges Faced and Solutions

1. **Real-time computation of metrics for high-volume stock data:** Due to the large number of windows for each of the 240 stock symbols, excessive compute power and memory was being used, which made the system hang. In order to solve this, we optimized the number of sliding windows used for the various calculations, to reduce memory usage.
2. **Synchronizing data from stock ticks and news stream:** The news data is fetched from the API every 2 hours, but the stock data is updated every 5 mins. This led to an issue of synchronizing timestamps of the two streams, which was essential for inferring from XGBoost. We solved this by fetching the news data from the API by mentioning the start and end datetime.
3. **ML output alone lacks clarity:** The output of the ML model alone may not be sufficient for the user to trust upon, especially in financial decisions. To address this, our alerts include the underlying technical indicators (like MACD signals and volatility forecasts) alongside the prediction, giving users the transparent data they need to trust the alert.
4. **Reducing user screen time through smart alerts:** If users have to constantly monitor dashboards for potential price swings, they lose time and the system becomes inconvenient, defeating the purpose of intelligent automation. Thus we introduced event-triggered alerting, where notifications are sent only when the system predicts a significant expected price movement or risk event.

## User Interface: Mobile App Integration (In Development)

The mobile app integration (currently under development) is designed to let users receive personalized alerts. When the backend sends alert data via Firebase Cloud Messaging (FCM), the user's device receives the push notification in real-time.

Future iterations of the app will allow users to submit their portfolio details and preferences during onboarding. The app will then locally filter incoming FCM broadcasts, only displaying notifications if the stock matches the user's preferences or could significantly impact their portfolio financially. The home screen will display suggestions from the last 12 hours so users can act on any they missed. All portfolio data and preferences will be stored locally in the device's internal storage. *(See Appendix 1.3)*

## Lessons Learned

Working on this project taught us a lot about building real-time systems, ML engineering, and solving practical problems. We gained hands-on experience creating production-ready streaming systems and turning complex financial analysis into something people can actually use. Here's what we learned along the way:

1. **Real-Time Processing Makes a Huge Difference:** Traditional batch processing doesn't work in finance where decisions happen in seconds. Using Pathway for incremental computations showed us how much faster and cheaper stateful streaming is than rerunning everything from scratch.
2. **Real-World Data Is Messy:** Stock data and news feeds don't play nicely together—misaligned timestamps, missing values, different update rates, and out-of-order arrivals. This taught us the importance of buffers, graceful failure handling, and solid data validation.
3. **Predictions Alone Aren't Enough:** Just giving users predictions without context didn't work well. Including the underlying technical indicators and sentiment scores alongside the recommendations significantly improved transparency, showing us how critical explainability is for financial decisions.
4. **User Experience Is Everything:** We initially thought users would monitor dashboards constantly. Smart alerts taught us that convenience and workflow integration matter more than fancy algorithms—great tech is useless if it's hard to use.
5. **Breaking Things Into Modules Pays Off:** Separating data ingestion, computation, and recommendations made development smoother. We could work in parallel, debug easily, and set ourselves up for future features like options trading or forex support.

## Conclusion

Through this project, we built a real-time AI system that processes live stock market data and news to help with investment decisions. The system calculates financial indicators on the fly, forecasts price movements, and generates personalized, easy-to-understand trading recommendations.

We used Pathway's streaming engine with Kafka for data ingestion and ClickHouse for storage and model retraining. The system combines technical analysis, sentiment analysis, ARMA/GARCH forecasting, and XGBoost for predictions, structuring the output into clear, actionable alerts that users can easily understand. The result is a scalable platform that connects streaming data engineering with machine learning and explainable AI—a real-time solution that gives us a clear advantage over traditional batch-processing systems and demonstrates Pathway's power for high-impact financial applications.

## References

- **Pathway Documentation** - Pathway, 2024. [https://pathway.com/docs/](https://pathway.com/docs/)
- **Kafka Documentation** – Apache Kafka, 2024. [https://kafka.apache.org/documentation/](https://kafka.apache.org/documentation/)
- **ClickHouse Documentation** – ClickHouse Inc., 2024. [https://clickhouse.com/docs/](https://clickhouse.com/docs/)
- **XGBoost: Scalable and Portable Gradient Boosting Framework** - Chen, Tianqi & Carlos Guestrin (2016). Proceedings of KDD.
- **ARMA & GARCH Models for Time Series Forecasting** - Engle, Robert (1982). Autoregressive Conditional Heteroskedasticity. Box, George & Jenkins, Gwilym (1970). Time Series Analysis: Forecasting and Control.