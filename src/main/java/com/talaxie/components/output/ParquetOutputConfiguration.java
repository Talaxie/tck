/*
 * Développé par : Hervé Ciaravolo / Red-belt
 */
package com.talaxie.components.output;

import com.talaxie.components.dataset.ParquetOutputDataset;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.meta.Ui;

import java.io.Serializable;

@GridLayout({
        @GridLayout.Row("dataset") // visible dans l’onglet principal
})
@GridLayout(names = GridLayout.FormType.ADVANCED, value ={
        @GridLayout.Row("rowGroupSize"),
        @GridLayout.Row("pageSize"),
        @GridLayout.Row("dictionaryPageSize"),
        @GridLayout.Row("enableDictionary")
})
@Documentation("Configuration avancée du composant ParquetOutput.")
public class ParquetOutputConfiguration implements Serializable {

    // =====================================================
    //    SECTION PRINCIPALE : Dataset
    // =====================================================
    @Option
    @Documentation("Dataset contenant le chemin du fichier Parquet et la configuration principale.")
    private ParquetOutputDataset dataset;

    public ParquetOutputDataset getDataset() {
        return dataset;
    }

    // =====================================================
    //    SECTION AVANCÉE : Parquet Tuning
    // =====================================================

    @Option
    @Documentation("Taille d'un RowGroup en octets. Défaut : 134217728 (128 Mo).")
    private long rowGroupSize = 134_217_728L;

    public long getRowGroupSize() {
        return rowGroupSize;
    }

    @Option
    @Documentation("Taille d'une page Parquet en octets. Défaut : 32768 (32 KB).")
    private int pageSize = 32_768;

    public int getPageSize() {
        return pageSize;
    }

    @Option
    @Documentation("Taille d'une page dictionnaire en octets. Défaut : 1048576 (1 Mo).")
    private int dictionaryPageSize = 1_048_576;

    public int getDictionaryPageSize() {
        return dictionaryPageSize;
    }

    @Option
    @Documentation("Active ou désactive l'encodage dictionnaire Parquet.")
    private boolean enableDictionary = true;

    public boolean isEnableDictionary() {
        return enableDictionary;
    }
}