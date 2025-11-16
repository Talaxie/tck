/*
 * Développé par : Hervé Ciaravolo / Red-belt
 */
package com.talaxie.components.source;

import com.talaxie.components.dataset.ParquetInputDataset;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;

public class ParquetInputMapperConfiguration {

    @Option
    @Documentation("Dataset Parquet pour la lecture.")
    private ParquetInputDataset dataset;

    public ParquetInputDataset getDataset() {
        return dataset;
    }

    public ParquetInputMapperConfiguration setDataset(ParquetInputDataset dataset) {
        this.dataset = dataset;
        return this;
    }
}