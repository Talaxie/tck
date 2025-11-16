/*
 * Développé par : Hervé Ciaravolo / Red-belt
 */
package com.talaxie.components.dataset;

import com.talaxie.components.datastore.ParquetDatastore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@DataSet("ParquetOutputDataset")
@GridLayout({
        @GridLayout.Row({ "datastore" }),
        @GridLayout.Row({ "path" }),
        @GridLayout.Row({ "compression" }),
        @GridLayout.Row({ "overwrite" })
})
@Documentation("Dataset Parquet d'écriture locale.")
public class ParquetOutputDataset implements Serializable {

    @Option
    private ParquetDatastore datastore;

    @Option
    @Documentation("Chemin complet du fichier parquet de sortie.")
    private String path;

    public enum Compression {
        UNCOMPRESSED,
        SNAPPY,
        GZIP
    }

    @Option
    @Documentation("Codec de compression Parquet.")
    private Compression compression = Compression.SNAPPY;

    @Option
    @Documentation("Écrase le fichier s’il existe déjà.")
    private boolean overwrite = false;

    public ParquetDatastore getDatastore() { return datastore; }
    public String getPath() { return path; }
    public Compression getCompression() { return compression; }
    public boolean isOverwrite() { return overwrite; }
}