package com.talaxie.components.source;

import com.talaxie.components.dataset.ParquetInputDataset;
import com.talaxie.components.datastore.ParquetDatastore;
import com.talaxie.components.service.Compression;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.*;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

public class ParquetInputSourceTest {

    @Rule
    public final SimpleComponentRule components = new SimpleComponentRule("com.talaxie.components");

    private File testParquetFile;


    /**
     * Méthode utilitaire pour créer un fichier Parquet de test en respectant l'option overwrite
     */
    private void createTestParquetFile(File file, Schema schema, boolean overwrite,
                                       Compression compression,
                                       int recordCount) throws IOException {
        Path path = new Path(file.getAbsolutePath());
        Configuration hadoopConf = new Configuration();

        // Convertir la compression du dataset en CompressionCodecName
        CompressionCodecName codecName;
        switch (compression) {
            case GZIP:
                codecName = CompressionCodecName.GZIP;
                break;
            case SNAPPY:
                codecName = CompressionCodecName.SNAPPY;
                break;
            case UNCOMPRESSED:
            default:
                codecName = CompressionCodecName.UNCOMPRESSED;
                break;
        }

        // Configurer le mode d'écriture selon l'option overwrite
        ParquetFileWriter.Mode writeMode = overwrite
                ? ParquetFileWriter.Mode.OVERWRITE
                : ParquetFileWriter.Mode.CREATE;

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(path)
                .withSchema(schema)
                .withConf(hadoopConf)
                .withCompressionCodec(codecName)
                .withWriteMode(writeMode)
                .build()) {

            // Écrire les enregistrements de test
            for (int i = 1; i <= recordCount; i++) {
                GenericRecord record = new GenericData.Record(schema);

                // Remplir seulement les champs qui existent dans le schéma
                for (Schema.Field field : schema.getFields()) {
                    switch (field.name()) {
                        case "id":
                            record.put("id", i);
                            break;
                        case "name":
                            record.put("name", "Person" + i);
                            break;
                        case "age":
                            record.put("age", 20 + i);
                            break;
                        case "salary":
                            record.put("salary", 30000.0 + (i * 1000));
                            break;
                    }
                }

                writer.write(record);
            }
        }
    }


    @Before
    public void setUp() throws IOException {
        // Créer un fichier Parquet temporaire pour les tests
        testParquetFile = Files.createTempFile("test-parquet", ".parquet").toFile();
        testParquetFile.deleteOnExit();

        // Créer un schéma Avro simple
        String schemaString = "{"
                + "\"type\":\"record\","
                + "\"name\":\"TestRecord\","
                + "\"fields\":["
                + "  {\"name\":\"id\",\"type\":\"int\"},"
                + "  {\"name\":\"name\",\"type\":\"string\"},"
                + "  {\"name\":\"age\",\"type\":\"int\"},"
                + "  {\"name\":\"salary\",\"type\":\"double\"}"
                + "]}";

        Schema avroSchema = new Schema.Parser().parse(schemaString);

        // Utiliser la méthode utilitaire avec overwrite=true
        createTestParquetFile(testParquetFile, avroSchema, true,
                Compression.SNAPPY, 5);
    }

    @After
    public void tearDown() {
        if (testParquetFile != null && testParquetFile.exists()) {
            testParquetFile.delete();
        }
    }

    @Test
    public void testReadParquetFile() {
        // Configuration du dataset
        final ParquetDatastore datastore = new ParquetDatastore();
        datastore.setBasePath("/");

        final ParquetInputDataset dataset = new ParquetInputDataset();
        dataset.setDatastore(datastore);
        dataset.setPath(testParquetFile.getAbsolutePath());

        final ParquetInputMapperConfiguration configuration = new ParquetInputMapperConfiguration();
        configuration.setDataset(dataset);

        // Création du job de lecture
        final String configUri = configurationByExample().forInstance(configuration).configured().toQueryString();

        Job.components()
                .component("input", "Talaxie://ParquetInput?" + configUri)
                .component("collector", "test://collector")
                .connections()
                .from("input").to("collector")
                .build()
                .run();

        // Vérification des résultats
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull("Les enregistrements ne doivent pas être null", records);
        assertEquals("On devrait avoir 5 enregistrements", 5, records.size());

        // Vérifier le premier enregistrement avec les TYPES corrects
        Record firstRecord = records.get(0);
        assertNotNull(firstRecord);

        // Vérifier les types (pas en String!)
        assertEquals("L'ID devrait être 1", 1, firstRecord.getInt("id"));
        assertEquals("Le nom devrait être Person1", "Person1", firstRecord.getString("name"));
        assertEquals("L'âge devrait être 21", 21, firstRecord.getInt("age"));
        assertEquals("Le salaire devrait être 31000.0", 31000.0, firstRecord.getDouble("salary"), 0.01);

        // Afficher les données pour vérification visuelle
        System.out.println("=== Données lues depuis le fichier Parquet ===");
        for (int i = 0; i < records.size(); i++) {
            Record record = records.get(i);
            System.out.println("Record " + (i + 1) + ": " +
                    "id=" + record.getInt("id") +
                    ", name=" + record.getString("name") +
                    ", age=" + record.getInt("age") +
                    ", salary=" + record.getDouble("salary"));
        }
    }

    @Test
    public void testReadEmptyParquetFile() throws IOException {
        // Créer un fichier Parquet vide
        File emptyFile = Files.createTempFile("empty-parquet", ".parquet").toFile();
        emptyFile.deleteOnExit();

        String schemaString = "{\"type\":\"record\",\"name\":\"EmptyRecord\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}";
        Schema avroSchema = new Schema.Parser().parse(schemaString);

        // Utiliser la méthode utilitaire avec overwrite=true et 0 enregistrements
        createTestParquetFile(emptyFile, avroSchema, true,
                Compression.SNAPPY, 0);

        // Configuration
        final ParquetDatastore datastore = new ParquetDatastore();
        datastore.setBasePath("/");

        final ParquetInputDataset dataset = new ParquetInputDataset();
        dataset.setDatastore(datastore);
        dataset.setPath(emptyFile.getAbsolutePath());

        final ParquetInputMapperConfiguration configuration = new ParquetInputMapperConfiguration();
        configuration.setDataset(dataset);

        final String configUri = configurationByExample().forInstance(configuration).configured().toQueryString();

        Job.components()
                .component("input", "Talaxie://ParquetInput?" + configUri)
                .component("collector", "test://collector")
                .connections()
                .from("input").to("collector")
                .build()
                .run();

        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull("Les enregistrements ne doivent pas être null", records);
        assertEquals("Un fichier vide devrait retourner 0 enregistrements", 0, records.size());

        emptyFile.delete();
    }

    @Test(expected = IllegalStateException.class)
    public void testReadNonExistentFile() {
        // Configuration avec un fichier qui n'existe pas
        final ParquetDatastore datastore = new ParquetDatastore();
        datastore.setBasePath("/");

        final ParquetInputDataset dataset = new ParquetInputDataset();
        dataset.setDatastore(datastore);
        dataset.setPath("/chemin/inexistant/fichier.parquet");

        final ParquetInputMapperConfiguration configuration = new ParquetInputMapperConfiguration();
        configuration.setDataset(dataset);

        final String configUri = configurationByExample().forInstance(configuration).configured().toQueryString();

        // Cela devrait lever une exception
        Job.components()
                .component("input", "Talaxie://ParquetInput?" + configUri)
                .component("collector", "test://collector")
                .connections()
                .from("input").to("collector")
                .build()
                .run();
    }

    @Test
    public void testReadWithDifferentCompressions() throws IOException {
        // Test avec compression GZIP
        File gzipFile = Files.createTempFile("test-gzip", ".parquet").toFile();
        gzipFile.deleteOnExit();

        String schemaString = "{"
                + "\"type\":\"record\","
                + "\"name\":\"TestRecord\","
                + "\"fields\":["
                + "  {\"name\":\"id\",\"type\":\"int\"},"
                + "  {\"name\":\"name\",\"type\":\"string\"}"
                + "]}";
        Schema schema = new Schema.Parser().parse(schemaString);

        createTestParquetFile(gzipFile, schema, true,
                Compression.GZIP, 3);

        // Configuration
        final ParquetDatastore datastore = new ParquetDatastore();
        datastore.setBasePath("/");

        final ParquetInputDataset dataset = new ParquetInputDataset();
        dataset.setDatastore(datastore);
        dataset.setPath(gzipFile.getAbsolutePath());
        dataset.setCompression(Compression.GZIP);

        final ParquetInputMapperConfiguration configuration = new ParquetInputMapperConfiguration();
        configuration.setDataset(dataset);

        final String configUri = configurationByExample().forInstance(configuration).configured().toQueryString();

        Job.components()
                .component("input", "Talaxie://ParquetInput?" + configUri)
                .component("collector", "test://collector")
                .connections()
                .from("input").to("collector")
                .build()
                .run();

        final List<Record> records = components.getCollectedData(Record.class);
        assertEquals("Devrait lire 3 enregistrements avec compression GZIP", 3, records.size());

        gzipFile.delete();
    }
}