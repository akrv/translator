/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.phynetlab.translator;

import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.eq;
import com.mongodb.client.result.UpdateResult;
import java.util.Arrays;
import org.bson.Document;
import org.kohsuke.randname.RandomNameGenerator;


/**
 *
 * @author akrv
 */
public class translator {
    static RandomNameGenerator rnd = new RandomNameGenerator(0);
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Config cfg = new Config(); 
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
        IMap<String, String> mapSensors = instance.getMap("mapSensors");
        IMap<String, String> snifferMap = instance.getMap("snifferMap");
        
        mapSensors.addEntryListener( new MyEntryListener(), true );
        snifferMap.addEntryListener( new MyEntryListener(), true );
        System.out.println( "EntryListener registered" );
    }
    
    static class MyEntryListener implements EntryAddedListener<String, String> {
        @Override
        public void entryAdded(EntryEvent<String, String> event ) {
        MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
        MongoDatabase db = mongoClient.getDatabase("translator-dev");
        MongoCollection<Document> collection = db.getCollection("nodes");
        
        
        String keysz,nodeAddr, split,name;
        split = event.getValue().replace("[","").replace("]","");
        String[] splitted = split.split(",");
        name = event.getName();
        if ("mapSensors".equals(name)){
            nodeAddr = splitted[6].replace(" ","");
        }else{
            nodeAddr = splitted[7].replace(" ","");
        }
        String connectedTo = splitted[5].replace(" ","");
        String lastReceived = event.getKey();
        String[] timeStamp = lastReceived.split("\\.");
        
        
        Document embeddedValues;
        
        FindIterable<Document> iterable = collection.find(eq("nodeAddr",nodeAddr)); 
        switch (name) {
                case "snifferMap":
//                System.out.println( "Entry Added:" + event );
//                FindIterable<Document> iterable = collection.find(eq("nodeAddr",nodeAddr));    
                if (iterable.first() == null) {
                    System.out.println("First: "+nodeAddr);
                String nameNode = rnd.next(); 
                
                embeddedValues = GenerateEmbeddedValues(timeStamp,splitted,lastReceived);
                
                Document document = new Document ("name",nameNode+"_sniffer")
                        .append("nodeAddr", nodeAddr)
                        .append("connectedTo", connectedTo)
                        .append("lastReceived", lastReceived)
                        .append("nodeType", "sniffer")
                        .append("payload", new Document (lastReceived.replace(".", ""), embeddedValues));
                collection.insertOne(document);
                }
                else{
                System.out.println("Put: "+nodeAddr);
                Document first = iterable.first();
                Document get = (Document) first.get("payload");
                
            // embedded document for the details of the field values
                embeddedValues = GenerateEmbeddedValues(timeStamp,splitted,lastReceived);
                keysz = "payload."+lastReceived.replace(".", "");
                get.append(lastReceived.replace(".", ""), embeddedValues);
                collection.updateOne(new Document("nodeAddr",nodeAddr), 
                                                            new Document("$set", new Document("lastReceived",lastReceived))
                                                            .append("$set", new Document(keysz,embeddedValues)) 
                                        );
                UpdateResult updateOne = collection.updateOne(new Document("nodeAddr",nodeAddr), 
                                                            new Document("$set", new Document("lastReceived",lastReceived))
                                                            .append("$currentDate", new Document("lastModified", true))
                                                                );
                }
                    break;
                case "mapSensors":
                       
                if (iterable.first() == null) {
                    System.out.println("First: "+nodeAddr);
                String nameNode = rnd.next(); 
                
                embeddedValues = GenerateEmbeddedValues(timeStamp,splitted,lastReceived);
                
                Document document = new Document ("name",nameNode+"_sensor")
                        .append("nodeAddr", nodeAddr)
                        .append("connectedTo", connectedTo)
                        .append("lastReceived", lastReceived)
                        .append("nodeType", "sensor")
                        .append("payload", new Document (lastReceived.replace(".", ""), embeddedValues));
                collection.insertOne(document);
                }
                else{
                System.out.println("Put: "+nodeAddr);
                Document first = iterable.first();
                Document get = (Document) first.get("payload");
                
            // embedded document for the details of the field values
                embeddedValues = GenerateEmbeddedValues(timeStamp,splitted,lastReceived);
                keysz = "payload."+lastReceived.replace(".", "");
                get.append(lastReceived.replace(".", ""), embeddedValues);
                collection.updateOne(new Document("nodeAddr",nodeAddr), 
                                                            new Document("$set", new Document("lastReceived",lastReceived))
                                                            .append("$set", new Document(keysz,embeddedValues)) 
                                        );
                UpdateResult updateOne = collection.updateOne(new Document("nodeAddr",nodeAddr), 
                                                            new Document("$set", new Document("lastReceived",lastReceived))
                                                            .append("$currentDate", new Document("lastModified", true))
                                                                );
                }
                    
                    break;
            }
        mongoClient.close();
    }
        private static Document GenerateEmbeddedValues(String[] timeStamp,String[] splitted, String lastReceived){
                        Document embeddedValues = new Document ("values",Arrays.toString(Arrays.copyOfRange(splitted, 0, 4)).replace(" ",""))
                        .append("year",timeStamp[0])
                        .append("month",timeStamp[1])
                        .append("date",timeStamp[2])
                        .append("hour",timeStamp[3])
                        .append("minute",timeStamp[4])
                        .append("second",timeStamp[5])
                        .append("millisecond",timeStamp[6])
                        .append("RSSI","22")
                        .append("timeStamp",lastReceived);
            return embeddedValues;
        }
  }
    
}