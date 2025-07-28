import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class GDELTCollector {
    private final String baseUrl;

    GDELTCollector() {
        baseUrl = "http://data.gdeltproject.org/gdeltv2/";
    }

    public Iterable<CSVRecord> processCSV(String[] inputFields, String dtSuffix) {
        String fullUrl = baseUrl + dtSuffix + ".gkg.csv.zip";
        URI uri;
        URL url;
        try {
            uri = new URI(fullUrl);
            url = uri.toURL();
        } catch(MalformedURLException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
        
        try (ZipInputStream zis = new ZipInputStream(url.openStream())) {
            ZipEntry entry = zis.getNextEntry();
            if (entry == null) {
                throw new IOException("No entries found in zip file: " + fullUrl);
            }
            try (Reader reader = new InputStreamReader(zis)) {
                CSVParser csvParser = CSVFormat.TDF.builder()
                        .setHeader(inputFields)
                        .setQuote(null)
                        .get()
                        .parse(reader);
                return csvParser.getRecords();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}