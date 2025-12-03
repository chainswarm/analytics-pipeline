from pathlib import Path
from typing import List, Dict
import pandas as pd
import json
import hashlib

from chainswarm_core import ClientFactory
from chainswarm_core.db import get_connection_params
from loguru import logger
from datetime import datetime, timedelta, timezone
import os
from celery_singleton import Singleton
from chainswarm_core.jobs import BaseTask, BaseTaskContext

from packages.storage import DATABASE_PREFIX

PROJECT_ROOT = Path(__file__).resolve().parents[3]

from packages.jobs.celery_app import celery_app
from packages.storage.repositories.feature_repository import FeatureRepository
from packages.storage.repositories.money_flows_repository import MoneyFlowsRepository
from packages.storage.repositories.address_label_repository import AddressLabelRepository
from packages.storage.repositories.structural_pattern_repository import StructuralPatternRepository


class ExportBatchTask(BaseTask, Singleton):

    def execute_task(self, context: BaseTaskContext):
        connection_params = get_connection_params(context.network, database_prefix=DATABASE_PREFIX)
        
        base_path = Path(os.getenv('BATCH_EXPORT_PATH', str(PROJECT_ROOT / 'data' / 'batches')))
        
        s3_enabled = os.getenv('RISK_SCORING_S3_ENABLED', 'false').lower() == 'true'
        s3_endpoint = os.getenv('RISK_SCORING_S3_ENDPOINT')
        s3_access_key = os.getenv('RISK_SCORING_S3_ACCESS_KEY')
        s3_secret_key = os.getenv('RISK_SCORING_S3_SECRET_KEY')
        s3_bucket = os.getenv('RISK_SCORING_S3_BUCKET')
        s3_region = os.getenv('RISK_SCORING_S3_REGION', 'us-east-1')

        client_factory = ClientFactory(connection_params)
        with client_factory.client_context() as client:
            features_repository = FeatureRepository(client)
            money_flows_repository = MoneyFlowsRepository(client)
            address_label_repository = AddressLabelRepository(client)
            patterns_repository = StructuralPatternRepository(client)
            
            logger.info(f"Exporting batch: {context.network}/{context.processing_date}/{context.window_days}d")
            
            export_dir = self._get_export_path(base_path, context.network, context.processing_date, context.window_days)
            export_dir.mkdir(parents=True, exist_ok=True)
            
            labels_export_dir = base_path / 'risk-scoring' / 'address-labels'
            labels_export_dir.mkdir(parents=True, exist_ok=True)
            
            features = self._load_features(features_repository, context.processing_date, context.window_days)
            money_flows = self._load_money_flows(money_flows_repository, context.processing_date, context.window_days)
            address_labels = self._load_address_labels(address_label_repository, context.network)
            patterns = self._load_patterns(patterns_repository, context.processing_date, context.window_days)
            
            file_paths = {}
            file_paths['features'] = self._export_parquet(features, export_dir / 'features.parquet')
            file_paths['money_flows'] = self._export_parquet(money_flows, export_dir / 'money_flows.parquet')
            file_paths['patterns'] = self._export_parquet(patterns, export_dir / 'patterns.parquet')
            
            labels_file_path = labels_export_dir / f'{context.network}_address_labels.parquet'
            file_paths['address_labels'] = self._export_parquet(address_labels, labels_file_path)
            
            meta = self._generate_metadata(
                context.network, context.processing_date, context.window_days,
                features, money_flows, address_labels, patterns,
                file_paths
            )
            
            meta_path = export_dir / 'META.json'
            with open(meta_path, 'w') as f:
                json.dump(meta, f, indent=2)
            
            logger.info(f"Batch exported to {export_dir}")
            logger.info(f"Address labels exported to {labels_file_path}")
            
            s3_uploaded = False
            s3_labels_uploaded = False
            if s3_enabled:
                if not all([s3_endpoint, s3_access_key, s3_secret_key, s3_bucket]):
                    logger.error("S3 upload enabled but missing required S3 credentials")
                    raise ValueError("Missing required S3 configuration")
                
                try:
                    self._upload_to_s3(
                        s3_endpoint, s3_access_key, s3_secret_key, s3_bucket, s3_region,
                        export_dir, context.network, context.processing_date, context.window_days
                    )
                    s3_uploaded = True
                    
                    s3_labels_uploaded = self._upload_address_labels_to_s3(
                        s3_endpoint, s3_access_key, s3_secret_key, s3_bucket, s3_region,
                        labels_file_path, context.network
                    )
                except Exception as e:
                    logger.error(f"S3 upload failed: {e}")
                    raise
            
            return {
                'export_path': str(export_dir),
                'labels_export_path': str(labels_file_path),
                'meta': meta,
                's3_uploaded': s3_uploaded,
                's3_labels_uploaded': s3_labels_uploaded
            }
    
    def _get_export_path(
        self,
        base_path: Path,
        network: str,
        processing_date: str,
        window_days: int
    ) -> Path:
        return base_path / 'risk-scoring' / 'snapshots' / network / processing_date / str(window_days)

    def _load_features(self, features_repository: FeatureRepository, processing_date: str, window_days: int) -> List[Dict]:
        logger.info("Loading features")
        
        features = features_repository.get_all_features(
            window_days=window_days,
            processing_date=processing_date,
            limit=1_000_000
        )
        
        if not features:
            raise ValueError(f"No features found for window_days={window_days}, processing_date={processing_date}")
        
        available_cols = list(features[0].keys())
        export_cols = [col for col in available_cols if col != '_version']
        
        filtered_features = [
            {col: feature[col] for col in export_cols}
            for feature in features
        ]
        
        logger.info(f"Loaded {len(filtered_features)} feature rows")
        return filtered_features

    def _load_money_flows(self, money_flows_repository: MoneyFlowsRepository, processing_date: str, window_days: int) -> List[Dict]:
        logger.info("Loading money flows")
        
        date_obj = datetime.strptime(processing_date, '%Y-%m-%d')
        end_timestamp = int(date_obj.timestamp() * 1000)
        start_timestamp = int((date_obj - timedelta(days=window_days)).timestamp() * 1000)
        
        money_flows = money_flows_repository.get_windowed_flows_from_transfers(
            start_timestamp_ms=start_timestamp,
            end_timestamp_ms=end_timestamp,
            limit=1_000_000
        )
        
        if not money_flows:
            raise ValueError(f"No money flows found for window_days={window_days}, processing_date={processing_date}")
        
        available_cols = list(money_flows[0].keys())
        export_cols = [col for col in available_cols if col != '_version']
        
        filtered_flows = [
            {**{col: flow[col] for col in export_cols}, 'processing_date': processing_date, 'window_days': window_days}
            for flow in money_flows
        ]
        
        logger.info(f"Loaded {len(filtered_flows)} money flow edges")
        return filtered_flows
    
    def _load_address_labels(self, address_label_repository: AddressLabelRepository, network: str) -> List[Dict]:
        logger.info("Loading address labels")
        
        labels = address_label_repository.get_all_labels(network=network)
        
        if not labels:
            logger.warning(f"No address labels found for network={network}")
            return []
        
        available_cols = list(labels[0].keys())
        export_cols = [col for col in available_cols if col != '_version']
        
        filtered_labels = [
            {col: label[col] for col in export_cols}
            for label in labels
        ]
        
        logger.info(f"Loaded {len(filtered_labels)} address labels")
        return filtered_labels

    def _load_patterns(self, patterns_repository: StructuralPatternRepository, processing_date: str, window_days: int) -> List[Dict]:
        logger.info("Loading patterns")
        
        patterns = patterns_repository.get_deduplicated_patterns(
            window_days=window_days,
            processing_date=processing_date
        )
        
        if not patterns:
            logger.warning(f"No patterns found for window_days={window_days}, processing_date={processing_date}")
            return []
        
        available_cols = list(patterns[0].keys())
        export_cols = [col for col in available_cols if col != '_version']
        
        filtered_patterns = [
            {col: pattern[col] for col in export_cols}
            for pattern in patterns
        ]
        
        logger.info(f"Loaded {len(filtered_patterns)} patterns")
        return filtered_patterns
    
    def _export_parquet(self, data: List[Dict], path: Path) -> str:
        df = pd.DataFrame(data)
        df.to_parquet(path, index=False, compression='snappy')
        logger.info(f"Exported {path.name}: {len(df)} rows")
        return str(path)
    
    def _generate_metadata(
        self,
        network: str,
        processing_date: str,
        window_days: int,
        features: List[Dict],
        money_flows: List[Dict],
        address_labels: List[Dict],
        patterns: List[Dict],
        file_paths: dict
    ) -> dict:
        logger.info("Generating metadata")
        
        hashes = {}
        for name, path in file_paths.items():
            hashes[f"{name}.parquet"] = self._compute_file_hash(path)
        
        meta = {
            'schema_version': '1.0.0',
            'batch_id': f"{network}-{processing_date}-{window_days}d",
            'network': network,
            'processing_date': processing_date,
            'window_days': window_days,
            'generated_at': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            
            'counts': {
                'features': len(features),
                'money_flows': len(money_flows),
                'address_labels': len(address_labels),
                'patterns': len(patterns)
            },
            
            'sha256': hashes
        }
        
        return meta
    
    def _compute_file_hash(self, file_path: str) -> str:
        sha256 = hashlib.sha256()
        
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                sha256.update(chunk)
        
        return sha256.hexdigest()
    
    def _upload_to_s3(
        self,
        s3_endpoint: str,
        s3_access_key: str,
        s3_secret_key: str,
        s3_bucket: str,
        s3_region: str,
        local_dir: Path,
        network: str,
        processing_date: str,
        window_days: int
    ):
        import boto3
        from botocore.exceptions import ClientError
        
        logger.info(f"Uploading risk scoring batch to S3: {s3_bucket}")
        
        s3 = boto3.client(
            's3',
            endpoint_url=s3_endpoint,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
            region_name=s3_region
        )
        
        s3_prefix = f"snapshots/{network}/{processing_date}/{window_days}"
        
        uploaded_count = 0
        for file_path in local_dir.glob('*'):
            if file_path.is_file():
                s3_key = f"{s3_prefix}/{file_path.name}"
                
                file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                logger.info(f"Uploading {file_path.name} to s3://{s3_bucket}/{s3_key} ({file_size_mb:.2f} MB)")
                
                try:
                    s3.upload_file(
                        str(file_path),
                        s3_bucket,
                        s3_key,
                        ExtraArgs={'ACL': 'public-read'}
                    )
                    logger.info(f"Uploaded {file_path.name} ({file_size_mb:.2f} MB)")
                    uploaded_count += 1
                except ClientError as e:
                    logger.error(f"Failed to upload {file_path.name}: {e}")
                    raise
        
        logger.success(f"Upload completed: {uploaded_count} files uploaded to s3://{s3_bucket}/{s3_prefix}")
    
    def _upload_address_labels_to_s3(
        self,
        s3_endpoint: str,
        s3_access_key: str,
        s3_secret_key: str,
        s3_bucket: str,
        s3_region: str,
        local_file_path: Path,
        network: str
    ) -> bool:
        import boto3
        from botocore.exceptions import ClientError
        
        logger.info(f"Preparing to upload address labels to S3")
        
        s3 = boto3.client(
            's3',
            endpoint_url=s3_endpoint,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
            region_name=s3_region
        )
        
        s3_key = f"address-labels/{network}_address_labels.parquet"
        local_hash = self._compute_file_hash(str(local_file_path))
        
        try:
            response = s3.head_object(Bucket=s3_bucket, Key=s3_key)
            s3_etag = response.get('ETag', '').strip('"')
            
            try:
                response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
                s3_content = response['Body'].read()
                s3_hash = hashlib.sha256(s3_content).hexdigest()
                
                if s3_hash == local_hash:
                    logger.info(f"Address labels file unchanged, skipping upload: {s3_key}")
                    return False
            except Exception as e:
                logger.warning(f"Could not compute S3 file hash, will upload: {e}")
        
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.info(f"Address labels file does not exist on S3, uploading: {s3_key}")
            else:
                logger.warning(f"Error checking S3 file, will upload: {e}")
        
        file_size_mb = os.path.getsize(local_file_path) / (1024 * 1024)
        logger.info(f"Uploading address labels to s3://{s3_bucket}/{s3_key} ({file_size_mb:.2f} MB)")
        
        try:
            s3.upload_file(
                str(local_file_path),
                s3_bucket,
                s3_key,
                ExtraArgs={'ACL': 'public-read'}
            )
            logger.success(f"Uploaded address labels ({file_size_mb:.2f} MB)")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload address labels: {e}")
            raise


@celery_app.task(
    bind=True,
    base=ExportBatchTask,
    autoretry_for=(Exception,),
    retry_kwargs={
        'max_retries': 24,
        'countdown': 600
    },
    time_limit=7200,
    soft_time_limit=7080
)
def export_batch_task(
    self,
    network: str,
    window_days: int,
    processing_date: str
):
    context = BaseTaskContext(
        network=network,
        window_days=window_days,
        processing_date=processing_date
    )
    
    return self.run(context)

