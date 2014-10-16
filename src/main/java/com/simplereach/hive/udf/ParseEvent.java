package com.simplereach.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.security.MessageDigest;
import java.util.*;
import javax.xml.bind.DatatypeConverter;
import com.simplereach.utils.UUIDs;


/**
 * Created by rbradberry on 10/14/14.
 */
public final class ParseEvent extends UDF {

    public Map<String, String> evaluate(String timestamp, String event) {

        if (event == null) {
            return null;
        }

        try {
            // Create our return object
            HashMap<String, String> map = new HashMap<>();

            //use the passed in timestamp to create a time uuid as the event id
            Calendar dateTime = DatatypeConverter.parseDateTime(timestamp);
            UUID eventId = UUIDs.timeBased(dateTime.getTimeInMillis());

            map.put("event_id", eventId.toString());

            //parse the url
            URL url = new URL(event);

            // get the event type based on the path of the url
            String path = url.getPath();
            String eventType;

            switch(path){
                case "/event":
                    eventType = "social_data";
                    break;
                case "/n":
                    eventType = "page_view";
                    break;
                case "/c":
                    eventType = "conversion";
                    break;
                case "/t":
                    eventType = "time_on_site";
                    break;
                default:
                    eventType = path;
            }

            map.put("event_type", eventType);

            //parse the query string
            String query = url.getQuery();
            String[] pairs = query.split("&");
            for (String pair : pairs) {
                int idx = pair.indexOf("=");
                if(idx == -1){
                    continue;
                }

                String key = URLDecoder.decode(pair.substring(0, idx), "UTF-8");
                String value = URLDecoder.decode(pair.substring(idx + 1), "UTF-8");

                if (value.equals("null") || value.equals("undefined")){
                    value = "";
                }

                //we don't care for the callback/inframe parameter
                if (key.equals("cb") || key.equals("inframe") || key.equals("iframe") || key.equals("cache_buster")){
                    continue;
                }

                // rename some of the common keys to be in their normalized names
                switch(key){
                    case "pid":
                        key = "account_id";
                        break;
                    case "cid":
                        key = "content_id";
                        break;
                    case "url":
                        key = "content_url";
                        break;
                    case "date":
                        if(eventType.equals("page_view")){
                            key = "published_at";
                        }
                    case "e":
                        if(eventType.equals("time_on_site")){
                            key = "engaged_time";
                        }
                        break;
                    case "t":
                        if(eventType.equals("time_on_site")){
                            key = "active_time";
                        }

                        if(eventType.equals("conversion")){
                            key = "conversion_id";
                        }
                        break;
                    case "r":
                        if(eventType.equals("time_on_site")){
                            key = "returning";
                        }
                        break;
                    case "s":
                        if(eventType.equals("time_on_site")){
                            key = "scroll_depth";
                        }
                        break;
                    case "sn":
                        if(eventType.equals("social_data")){
                            key = "social_network";
                        }
                        break;
                    case "uid":
                        key = "user_id";
                        break;
                }

                if(eventType.equals("social_data")){
                    map.remove("event");
                }

                map.put(key, value);

                // if the content_id does not exist, create it as an md5 of the url
                if (map.get("content_id") == null && map.get("content_url") != null){
                    MessageDigest md = MessageDigest.getInstance("MD5");
                    md.update(map.get("content_url").getBytes());
                    byte[] md5hash = md.digest();
                    StringBuilder builder = new StringBuilder();
                    for (byte b : md5hash) {
                        builder.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
                    }

                    map.put("content_id", builder.toString());
                }

                //if the ref_url exists then overwrite
                String referrer = map.get("ref_url");
                if(referrer != null && referrer.length() > 0){
                    map.put("referrer", referrer);
                }

                map.remove("ref_url");
            }
            return map;
        } catch (MalformedURLException e) {
            System.out.println("MALFORMED URL");
            return null;
        } catch (UnsupportedEncodingException e) {
            System.out.println("UNSUPPORTED ENCODING");
            return null;
        } catch (Exception e){
            System.out.println("Unknown Exception");
            e.printStackTrace(System.out);
            return null;
        }
    }
}
