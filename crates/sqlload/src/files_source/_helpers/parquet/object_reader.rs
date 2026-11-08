use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::{FutureExt, TryFutureExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

use crate::files_source::constants::FOOTER_PREFETCH_BYTES;

/// Reads one Parquet file from an object store with range requests; its size is known from the listing.
#[derive(Debug, Clone)]
pub(crate) struct ObjectFileReader {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) path: Path,
    pub(crate) size: u64,
}

fn parquet_error(error: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}

impl AsyncFileReader for ObjectFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.store
            .get_range(&self.path, range)
            .map_err(parquet_error)
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        async move {
            self.store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(parquet_error)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let size = self.size;
            let metadata = ParquetMetaDataReader::new()
                .with_prefetch_hint(Some(FOOTER_PREFETCH_BYTES))
                .with_arrow_reader_options(options)
                .load_and_finish(self, size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}
