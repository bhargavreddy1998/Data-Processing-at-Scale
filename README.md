# Data Processing at Scale - Assignment Solutions

This repository contains my solutions to assignments from a Data Processing at Scale course, focusing on database design, SQL query optimization, data partitioning, and big data analytics using distributed systems.

## 📋 Course Assignments

### Assignment 1: Movie Recommendation Database Design
- **Task**: Design and implement a complete movie recommendation database from scratch
- **Schema Design**: 7-table relational database with proper normalization
  - `users`: User information and profiles
  - `movies`: Movie catalog with titles and IDs
  - `taginfo`: Tag content and metadata
  - `genres`: Movie genre categories
  - `ratings`: User ratings (0-5 scale) with timestamps
  - `tags`: User-generated tags for movies with timestamps
  - `hasagenre`: Many-to-many relationship between movies and genres
- **Features**: 
  - Movie rating system with timestamp logging
  - Multi-genre categorization support
  - User tagging system with multiple tags per movie
  - Data integrity constraints and foreign key relationships
- **Technologies**: PostgreSQL, SQL DDL
- **Deliverables**: Complete SQL schema with table creation scripts

### Assignment 2: Advanced SQL Query Implementation
- **Task**: Implement complex analytical queries on the movie database
- **Query Types**: 9 sophisticated queries covering various analytical scenarios
- **Key Queries**:
  - Genre-based movie statistics and average ratings
  - Movies with high engagement (10+ ratings)
  - Cross-genre analysis (Comedy vs Romance movies)
  - Parameterized user-specific movie retrieval
  - Complex filtering and aggregation operations
- **Output Management**: Results stored in dedicated query tables (query1, query2, etc.)
- **Skills Demonstrated**: JOIN operations, GROUP BY aggregations, subqueries, conditional filtering
- **Deliverables**: SQL scripts with query implementations and result table creation

### Assignment 3: Horizontal Data Partitioning
- **Task**: Implement scalable data partitioning strategies using Python and PostgreSQL
- **Dataset**: MovieLens dataset (10M ratings, 72K users, 10K movies)
- **Partitioning Strategies**:
  - **Range Partitioning**: Data distributed based on rating value ranges
  - **Round-Robin Partitioning**: Even distribution across partitions using modulo operation
- **Implementation Functions**:
  - `loadRatings()`: Load large datasets into PostgreSQL efficiently
  - `rangePartition()`: Create N partitions based on uniform rating ranges
  - `roundRobinPartition()`: Distribute data evenly across N partitions
  - `rangeinsert()` & `roundrobininsert()`: Insert new data into appropriate partitions
- **Scalability Features**: Dynamic partition management and insertion optimization
- **Technologies**: Python, PostgreSQL, psycopg2
- **Deliverables**: Complete Python interface for partition management

### Assignment 4: Query Processing on Partitioned Data
- **Task**: Build an efficient query processor for partitioned database systems
- **Query Functions**:
  - **RangeQuery()**: Retrieve data within specified rating ranges across all partitions
  - **PointQuery()**: Find exact rating matches across partition boundaries
- **Features**:
  - Cross-partition query execution
  - Result aggregation from multiple data sources
  - Output formatting with partition identification
  - Performance optimization for distributed queries
- **Output Format**: Structured text files with partition names and tuple data
- **Skills**: Distributed query processing, partition pruning, result merging
- **Deliverables**: Query processor with file-based result output

## 🏗 Data Analysis Projects

### Milestone 1: Business Intelligence Analysis
- **Objective**: Comprehensive business performance analysis using Yelp dataset
- **Scope**: Health & Medical sector businesses in Arizona
- **Analysis Framework**:
  - Business categorization and market distribution
  - Performance metrics (ratings, review counts)
  - Geographic analysis by cities and zip codes
  - Top-performing business identification
- **Tools**: Apache Spark, Spark SQL, PySpark
- **Insights**: Business location strategies, market penetration analysis, performance benchmarking
- **Deliverables**: Business intelligence report with data visualizations

### Milestone 2: User Behavior Analytics
- **Objective**: Deep-dive analysis of user engagement patterns and sentiment trends
- **Dataset**: Multi-dimensional Yelp data (reviews, users, businesses, tips, check-ins)
- **Analysis Categories**:
  - **Simple Queries**: Top users, average metrics, engagement statistics
  - **Complex Queries**: Sentiment analysis, influence mapping, temporal trends
- **Key Metrics**:
  - User contribution analysis (review counts, useful votes)
  - Sentiment correlation with user activity
  - Temporal trend analysis of user behavior
  - Geographic distribution of user engagement
- **Advanced Analytics**: Review length correlation with influence, sentiment stability over time
- **Tools**: Spark SQL, Python analytics libraries
- **Deliverables**: Comprehensive user behavior analysis with trend visualizations

## 🛠 Technical Stack

### Database Technologies
- **PostgreSQL**: Primary database for relational data storage
- **SQL**: Advanced query optimization and database design

### Programming Languages
- **Python**: Data processing, partition management, and analytics
- **SQL**: Database operations and complex query implementation

### Big Data Processing
- **Apache Spark**: Distributed data processing and analytics
- **PySpark**: Python API for Spark operations
- **Spark SQL**: SQL-based big data querying

### Development Tools
- **psycopg2**: PostgreSQL adapter for Python
- **Pandas/NumPy**: Data manipulation and analysis
- **Matplotlib/Seaborn**: Data visualization

## 📊 Key Achievements

### Database Design & Management
- Designed normalized database schema supporting complex relationships
- Implemented efficient data partitioning strategies for horizontal scaling
- Achieved optimized query performance across partitioned data

### Query Optimization
- Developed complex analytical queries with multiple JOIN operations
- Implemented cross-partition query processing capabilities
- Optimized data retrieval for large-scale datasets (10M+ records)

### Big Data Analytics
- Processed and analyzed large-scale Yelp dataset using distributed computing
- Implemented sophisticated business intelligence queries using Spark SQL
- Generated actionable insights for business performance optimization

### Performance Results
- Successfully partitioned 10M records across multiple database partitions
- Achieved efficient query processing across distributed data storage
- Implemented scalable solutions supporting real-time data insertion

## 🎯 Learning Outcomes

Through this course, I gained expertise in:

### Database Systems
- **Schema Design**: Normalized database design with proper relationships
- **Data Partitioning**: Horizontal scaling strategies for large datasets
- **Query Optimization**: Complex SQL operations and performance tuning

### Distributed Systems
- **Parallel Processing**: Data distribution and parallel query execution
- **Scalability**: Designing systems for horizontal scaling
- **Fault Tolerance**: Robust data management across multiple partitions

### Big Data Analytics
- **Spark Ecosystem**: Distributed data processing using Apache Spark
- **Business Intelligence**: Large-scale data analysis for business insights
- **Performance Optimization**: Query optimization for big data workloads

### Real-world Applications
- **Business Analytics**: Customer behavior and business performance analysis
- **Data Engineering**: ETL processes for large-scale data processing
- **System Design**: Scalable architecture for data-intensive applications

---

*This repository demonstrates comprehensive skills in database design, distributed systems, big data processing, and analytics - essential competencies for modern data engineering and data science roles.*