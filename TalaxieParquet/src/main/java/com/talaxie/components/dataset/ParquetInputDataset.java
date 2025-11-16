/*
 * Développé par : Hervé Ciaravolo / Red-belt
 */
package com.talaxie.components.dataset;

import java.io.Serializable;

import com.talaxie.components.datastore.ParquetDatastore;
import com.talaxie.components.service.Compression;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@DataSet("ParquetInputDataset")
@GridLayout({
        @GridLayout.Row({ "path" }),
        @GridLayout.Row({ "compression" })
})
@GridLayout(names = GridLayout.FormType.ADVANCED, value = {
        @GridLayout.Row({ "datastore" })
})
@Documentation("Dataset pour la lecture Parquet.")
public class ParquetInputDataset implements Serializable {

    @Option
    @Documentation("Datastore (non affiché)")
    private ParquetDatastore datastore;

    @Option
    @Required
    @Documentation("Chemin complet du fichier Parquet.")
    private String path;

    @Option
    @DefaultValue("SNAPPY")
    @Documentation("Compression détectée ou imposée (lecture).")
    private Compression compression;

    public ParquetDatastore getDatastore() {
        return datastore;
    }

    public void setDatastore(final ParquetDatastore datastore) {
        this.datastore = datastore;
    }

    public String getPath() {
        return path;
    }

    public ParquetInputDataset setPath(final String path) {
        this.path = path;
        return this;
    }

    public Compression getCompression() {
        return compression;
    }

    public void setCompression(final Compression compression) {
        this.compression = compression;
    }
}