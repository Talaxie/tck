/*
 * Développé par : Hervé Ciaravolo / Red-belt
 */
package com.talaxie.components.datastore;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.meta.Documentation;

@DataStore("ParquetDatastore")
@Documentation("Datastore Parquet simple, permettant de définir un chemin de base pour les fichiers Parquet.")
public class ParquetDatastore implements Serializable {

    @Option
    @Documentation("Chemin de base sur le système de fichiers où se trouvent les fichiers Parquet.")
    private String basePath;

    public String getBasePath() {
        return basePath;
    }

    public ParquetDatastore setBasePath(final String basePath) {
        this.basePath = basePath;
        return this;
    }
}