# Features Package - Refactored Architecture

This package implements a **focused analyzer architecture** following the `MoneyFlowAggregator` pattern.

## Architecture Pattern

### **Individual Analyzers** (Single Responsibility)
Each analyzer has one clear purpose and a single public method:

- **`AddressFeatureAnalyzer`** - Builds comprehensive ML features for addresses
- **`FeatureStatisticsAnalyzer`** - Computes statistical summaries for ML normalization  
- **`FeatureNormalizer`** - Exports ML-ready normalized features

### **Orchestrated Execution** (Task Coordination)
**`BuildFeaturesTask`** coordinates all analyzers in a mandatory sequence:

```python
# 1. Core feature building (required)
address_analyzer.analyze_address_features()

# 2. Statistical analysis (required for ML)
stats_analyzer.analyze_feature_statistics()

# 3. ML export (MANDATORY)
normalizer.export_ml_features(output_path=context.ml_output_path)
```

## Usage

### Via Celery Task (Recommended)
```python
from packages.jobs.tasks.build_features_task import build_features_task

# Execute complete feature pipeline
result = build_features_task.delay(
    network="torus",
    window_days=180,
    processing_date="2025-09-09",
    force_rebuild=False,
    batch_size=1000
)
```

### Direct Usage (Testing/Development)
```python
from packages.analyzers.features import (
    AddressFeatureAnalyzer,
    FeatureStatisticsAnalyzer, 
    FeatureNormalizer
)

# Setup repositories and config...

# Step 1: Build address features
address_analyzer = AddressFeatureAnalyzer(...)
address_analyzer.analyze_address_features()

# Step 2: Compute feature statistics  
stats_analyzer = FeatureStatisticsAnalyzer(...)
stats_analyzer.analyze_feature_statistics()

# Step 3: Export ML features
normalizer = FeatureNormalizer(...)
normalizer.export_ml_features(output_path="./features.parquet")
```

## Key Benefits

1. **Single Responsibility** - Each analyzer focuses on one task
2. **Clean Interfaces** - One main public method per analyzer
3. **Orchestrated Execution** - Task coordinates all analyzers
4. **Mandatory ML Export** - Ensures ML pipeline completion
5. **Consistent Pattern** - Follows MoneyFlowAggregator design

## Features Produced

### Core Features (40+ features per address)
- **Node Features**: degree_in, degree_out, degree_total, unique_counterparties
- **Volume Features**: total_in_usd, total_out_usd, net_flow_usd, total_volume_usd
- **Statistical Features**: amount_variance, amount_skewness, volume_std, volume_cv
- **Temporal Features**: activity_days, peak_hour, hourly_entropy, weekend_ratio
- **Flow Features**: reciprocity_ratio, flow_diversity, velocity_score
- **Behavioral Features**: structuring_score, consistency_score
- **Quality Features**: completeness_score, quality_score, outlier_score

### ML Output
- **Format**: Parquet (default), CSV, Pickle
- **Normalization**: Z-score normalized numeric features
- **Outlier Removal**: Automatic outlier filtering
- **Export Path**: `./ml_features/{network}/{date}_{window}d_features.parquet`

## Migration from Legacy FeatureBuilder

The new architecture replaces the monolithic `FeatureBuilder` with focused analyzers:

### Before (Legacy)
```python
feature_builder = FeatureBuilder(...)
feature_builder.build_all_features()  # Does everything
```

### After (Focused)
```python
# Use orchestrated task (recommended)
build_features_task.delay(network="torus", ...)

# Or use individual analyzers for testing
address_analyzer.analyze_address_features()
stats_analyzer.analyze_feature_statistics() 
normalizer.export_ml_features()
```

This provides better separation of concerns, easier testing, and consistent architecture across the codebase.