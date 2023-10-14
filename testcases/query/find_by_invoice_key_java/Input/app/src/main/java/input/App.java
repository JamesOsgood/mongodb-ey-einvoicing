package input;
import static com.mongodb.client.model.Filters.eq;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.conversions.Bson;
import org.bson.Document;
import com.mongodb.client.FindIterable;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.Level;

// Your First Program

class App {
    private Logger _logger;

    public static void main(String[] args) {
    }

    public App() throws IOException {
        _logger = Logger.getLogger(App.class.getName());
        // Create an instance of FileHandler that write log to a file called
        // app.log. Each new message will be appended at the at of the log file.
        FileHandler fileHandler = new FileHandler("app.log", false);
        this._logger.addHandler(fileHandler);

    }

    public boolean invoice_key_exists(String invoice_key) {
        Bson filter = eq("queryable.invoice_key", invoice_key);

        String connectionString = "mongodb://localhost:27017/";
        MongoClient mongoClient = MongoClients.create(connectionString);
        MongoDatabase database = mongoClient.getDatabase("ey");
        MongoCollection<Document> collection = database.getCollection("invoice");
        MongoCursor<Document> result = collection.find(filter).cursor();
        boolean exists = false;
        while (result.hasNext()){
            this._logger.info(result.next().toJson());
            exists = true;
        }
        return exists;
    }
}