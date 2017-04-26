package com.erik.learn;

import java.util.List;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class App {
  public static void main(String[] args) throws Exception {
    System.out.println("MongoDB Hello World!");

    MongoClient mongo = new MongoClient("localhost", 27017);
    listDataBases(mongo);
    separate();
    DB example = listCollectionsFromSelectedDatabase(mongo);
    separate();
    DBCollection carCollection = getCollection(example);
    separate();
    insertData(carCollection);
    separate();
    updateData(carCollection);
    separate();
    searchSpecificDataAndDelete(carCollection);
    separate();
    showAllData(carCollection);
  }

  private static void separate(){
    System.out.println();
    System.out.println();
  }

  private static void listDataBases(MongoClient mongo){
    List<String> dbs = mongo.getDatabaseNames();
    System.out.println("List of databases:");
    for (String db : dbs) {
      System.out.println("*> " + db);
    }
  }

  private static DB listCollectionsFromSelectedDatabase(MongoClient mongo) {
    DB db = mongo.getDB("example");

    Set<String> collections = db.getCollectionNames();
    System.out.println("List of collection in example:");
    for (String coll : collections) {
      System.out.println("*> " + coll);
    }
    return db;
  }

  private static DBCollection getCollection(DB db){
    System.out.println("get cars collection");
    DBCollection carCollection = db.getCollection("cars");
    return carCollection;
  }

  private static void insertData(DBCollection carCollection){
    BasicDBObject document = new BasicDBObject();
    document.put("name", "Nissan");
    document.put("year", 2010);
    document.put("type", "Skyline");
    carCollection.insert(document);
    System.out.println("Following data is inserted");
    System.out.println(document);
  }

  private static void updateData(DBCollection carCollection){
    BasicDBObject query = new BasicDBObject();
    query.put("name", "Nissan");

    BasicDBObject newDocument = new BasicDBObject();
    newDocument.put("name", "Nissan updated");

    BasicDBObject updateObj = new BasicDBObject();
    updateObj.put("$set", newDocument);

    carCollection.update(query, updateObj);
    System.out.println("Update data with query and data");
    System.out.println(query);
    System.out.println(updateObj);
  }

  private static void searchSpecificDataAndDelete(DBCollection carCollection){
    BasicDBObject searchQuery = new BasicDBObject();
    searchQuery.put("name", "Nissan updated");

    DBCursor cursor = carCollection.find(searchQuery);

    System.out.println("Search data with:");
    System.out.println(searchQuery);
    System.out.println();
    System.out.println("Result: ");
    while (cursor.hasNext()) {
      System.out.println("*> " + cursor.next());
    }

    System.out.println();
    System.out.println("delete data with query:");
    System.out.println(searchQuery);
    carCollection.remove(searchQuery);
  }

  private static void showAllData(DBCollection carCollection){
    DBCursor allDataCursor = carCollection.find(new BasicDBObject());

    System.out.println("Search data with no filter:");
    while (allDataCursor.hasNext()) {
      System.out.println("*> " + allDataCursor.next());
    }
  }

}
