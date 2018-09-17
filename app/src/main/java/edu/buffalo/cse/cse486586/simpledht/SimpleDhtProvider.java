package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.nfc.Tag;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.text.Selection;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {
    String myPort;
    int Final_port = 0;
    int Succ = 0;
    int Pred = 0;
    static final int SERVER_PORT = 10000;
    HashMap<String, String> local_key_value = new HashMap<String, String>();
    HashMap<String, String> Global_key_value = new HashMap<String, String>();
    Boolean Global_flag = false;
    Boolean key_found = false;
    HashMap<String, String> key_details = new HashMap<String, String>();


    private SQLiteDatabase dbase;
    static final String db_name = "Dht_db";
    static final String table_name = "basetable";
    static final int db_version = 1;
    static final String create_table = " CREATE TABLE " + table_name + " (key TEXT," + " value TEXT);";

    //helper class made by taking reference from https://developer.android.com/reference/android/database/sqlite/SQLiteOpenHelper.html
//    You create a subclass implementing onCreate(SQLiteDatabase), onUpgrade(SQLiteDatabase, int, int) and optionally onOpen(SQLiteDatabase),
// and this class takes care of opening the database if it exists, creating it if it does not, and upgrading it as necessary.

    private static class db_help extends SQLiteOpenHelper {
        db_help(Context context){
            super(context, db_name, null, db_version);
        }

        @Override
        public void onCreate(SQLiteDatabase dbase) {
            dbase.execSQL("DROP TABLE IF EXISTS " +  table_name);
            dbase.execSQL(create_table);

        }

        @Override
        public void onUpgrade(SQLiteDatabase dbase, int oldVersion, int newVersion) {
            dbase.execSQL("DROP TABLE IF EXISTS " +  table_name);
            onCreate(dbase);
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        try {
            if (selection.equals("@")) {
                local_key_value.clear();
            }

            else if (selection.equals("*")) {
                local_key_value.clear();
                String all_del = "Delete_all" + "~@#" + Final_port;
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Succ);
                DataOutputStream Output_stream = new DataOutputStream(socket.getOutputStream());
                Output_stream.writeUTF(all_del);
                Output_stream.flush();
                socket.close();
            }

            else {
                if (local_key_value.containsKey(selection)) {
                    local_key_value.remove(selection);
//                Log.v(TAG, selection + " was not found here");
                } else {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Succ);
                    String del_msg = "Delete_key" + "~@#" + selection + "~@#" + Final_port;
                    DataOutputStream Output_stream = new DataOutputStream(socket.getOutputStream());
                    Output_stream.writeUTF(del_msg);
                    Output_stream.flush();
//                    Log.v(TAG, selection + "sent to " + Succ);
                    socket.close();
                }
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        //reference https://developer.android.com/training/basics/data-storage/databases.html
        String KEY_FIELD = "key";
//        String keys[] = new String[100];
//        String value[] = new String[100];
        ArrayList<String> keys = new ArrayList<String>();
        ArrayList<String> value = new ArrayList<String>();
        HashMap<String, String> key_value = new HashMap<String, String>();
        for (String str : values.keySet()) {
            if (str.equals(KEY_FIELD)) {
                keys.add(values.getAsString(str));
            }
            else
                value.add(values.getAsString(str));
        }

        for (int i = 0; i< keys.size(); i++){
            key_value.put(keys.get(i), value.get(i));
        }

        for (String key : keys){
            try{
                String Contend = genHash(key);
                String current = genHash(String.valueOf(Final_port/2));
                String Succ_hash = genHash(String.valueOf(Succ/2));
                String Pred_hash = genHash(String.valueOf(Pred/2));

                if(Final_port == Succ && Final_port == Pred){
//                    Log.v(TAG, "this statement runs");
                    local_key_value.put(key, key_value.get(key));
//                    Log.v(TAG, local_key_value.get(key));
//                    Log.v(TAG, "This code runs");
                }

                else if(current.compareTo(Pred_hash) > 0){
                    if(Contend.compareTo(current) < 0  && Contend.compareTo(Pred_hash) > 0){
//                        keyValueToInsert.put("key",key);
//                        keyValueToInsert.put("value", key_value.get(key));
                        local_key_value.put(key, key_value.get(key));
                    }
                    else if (Contend.compareTo(current) < 0 && Contend.compareTo(Pred_hash) < 0){
//                        Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//                                Pred);
//                        String Insert_msg = "Insert" + "~@#" + key + "~@#" + key_value.get(key);
//                        DataOutputStream Output_stream_1 = new DataOutputStream(socket_1.getOutputStream());
//                        Output_stream_1.writeUTF(Insert_msg);
//                        Output_stream_1.flush();
//                        socket_1.close();
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Handle_Insert_Pred", myPort, key, key_value.get(key));
                    }
                    else if (Contend.compareTo(current) > 0 && Contend.compareTo(Pred_hash) > 0){
//                        Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//                                Succ);
//                        String Insert_msg = "Insert" + "~@#" + key + "~@#" + key_value.get(key);
//                        DataOutputStream Output_stream_1 = new DataOutputStream(socket_1.getOutputStream());
//                        Output_stream_1.writeUTF(Insert_msg);
//                        Output_stream_1.flush();
//                        socket_1.close();
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Handle_Insert_Succ", myPort, key, key_value.get(key));
                    }
                }

                else{ //Handling edge case.
                    if(Contend.compareTo(current) > 0 && Contend.compareTo(Pred_hash) < 0){
//                        Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//                                Succ);
//                        String Insert_msg = "Insert" + "~@#" + key + "~@#" + key_value.get(key);
//                        DataOutputStream Output_stream_1 = new DataOutputStream(socket_1.getOutputStream());
//                        Output_stream_1.writeUTF(Insert_msg);
//                        Output_stream_1.flush();
//                        socket_1.close();

                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Handle_Insert_Succ", myPort, key, key_value.get(key));
                    }
                    else if((Contend.compareTo(current) > 0 && Contend.compareTo(Pred_hash) > 0) || (Contend.compareTo(current) < 0 && Contend.compareTo(Pred_hash) < 0)){
//                        keyValueToInsert.put("key",key);
//                        keyValueToInsert.put("value", key_value.get(key));
                        local_key_value.put(key, key_value.get(key));
                    }
                }

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

//
//        db_help mDbHelper = new db_help(getContext());
//        SQLiteDatabase db = mDbHelper.getWritableDatabase();
//        if(keyValueToInsert.size() != 0) {
//            db.insert(table_name, null, keyValueToInsert);
//        }
            Log.v("insert", values.toString());
            return uri;
    }

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Final_port = Integer.parseInt(myPort);
        Succ = Final_port;
        Pred = Final_port;

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }

        if (Final_port != 11108){
            String msg = "Node_Join";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
        }

        // TODO Auto-generated method stub
        return false;
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Socket socket_server = null;
            try {

                while(true) {
                    //Code entirely similar to PA1.
                    socket_server = serverSocket.accept();
                    DataInputStream Server_input = new DataInputStream(socket_server.getInputStream());
                    String Input_string = Server_input.readUTF();
                    //final String Input_string = Server_input.toString();
                    String[] msg_type = Input_string.split("~@#");
//                    Log.v(TAG, Input_string);
//                    Log.v(TAG,"Initial condition" + msg_type[0]);



//                    Log.v(TAG, "Msg recieved :" +Input_string);
//                    Log.v(TAG, msg_type[0]);
                    if (msg_type[0].equals("Node_Join")){
                        int half_contend = Integer.parseInt(msg_type[1]);
                        DataOutputStream ack = new DataOutputStream(socket_server.getOutputStream());
                        String Contend = genHash(String.valueOf(half_contend / 2));
                        String current = genHash(String.valueOf(Final_port/2));
                        String Succ_hash = genHash(String.valueOf(Succ/2));
                        String Pred_hash = genHash(String.valueOf(Pred/2));
                        if(Final_port == Succ && Final_port == Pred){
                            Succ = Integer.parseInt(msg_type[1]);
                            Pred = Integer.parseInt(msg_type[1]);
                            String Update_succ_pred = "Initial" + "~@#" + myPort;
                            Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(msg_type[1]));
                            DataOutputStream Output_stream = new DataOutputStream(socket_1.getOutputStream());
                            Output_stream.writeUTF(Update_succ_pred);
                            Output_stream.flush();
                            socket_1.close();
//                            Log.v(TAG, "This code runs");
                        }
                        else if(current.compareTo(Pred_hash) > 0){
                            if(Contend.compareTo(current) < 0  && Contend.compareTo(Pred_hash) > 0){
                                int behind = Pred;
                                Pred = Integer.parseInt(msg_type[1]);
                                String Update_succ_pred = "Update_Succ_pred" + "~@#" + myPort + "~@#" + behind;
                                Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(msg_type[1]));
                                DataOutputStream Output_stream_1 = new DataOutputStream(socket_1.getOutputStream());
                                Output_stream_1.writeUTF(Update_succ_pred);
                                Output_stream_1.flush();
                                socket_1.close();

                                String Update_succ = "Update_Succ" + "~@#" + myPort + "~@#" + msg_type[1];
                                Socket socket_2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        behind);
                                DataOutputStream Output_stream_2 = new DataOutputStream(socket_2.getOutputStream());
                                Output_stream_2.writeUTF(Update_succ);
                                Output_stream_2.flush();
                                socket_2.close();
                            }
                            else if (Contend.compareTo(current) < 0 && Contend.compareTo(Pred_hash) < 0){
                                Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Pred);
                                String Node_join_msg = "Node_Join" + "~@#" + msg_type[1];
                                DataOutputStream Output_stream_1 = new DataOutputStream(socket_1.getOutputStream());
                                Output_stream_1.writeUTF(Node_join_msg);
                                Output_stream_1.flush();
                                socket_1.close();
                            }
                            else if (Contend.compareTo(current) > 0 && Contend.compareTo(Pred_hash) > 0){
                                Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Succ);
                                String Node_join_msg = "Node_Join" + "~@#" + msg_type[1];
                                DataOutputStream Output_stream_1 = new DataOutputStream(socket_1.getOutputStream());
                                Output_stream_1.writeUTF(Node_join_msg);
                                Output_stream_1.flush();
                                socket_1.close();
                            }
                        }
                        else{ //Handling edge case.
                            if(Contend.compareTo(current) > 0 && Contend.compareTo(Pred_hash) < 0){
                                Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Succ);
                                String Node_join_msg = "Node_Join" + "~@#" + msg_type[1];
                                DataOutputStream Output_stream_1 = new DataOutputStream(socket_1.getOutputStream());
                                Output_stream_1.writeUTF(Node_join_msg);
                                Output_stream_1.flush();
                                socket_1.close();
                            }
                            else if((Contend.compareTo(current) > 0 && Contend.compareTo(Pred_hash) > 0) || (Contend.compareTo(current) < 0 && Contend.compareTo(Pred_hash) < 0)){
                                int behind = Pred;
                                Pred = Integer.parseInt(msg_type[1]);
                                String Update_succ_pred = "Update_Succ_pred" + "~@#" + myPort + "~@#" + behind;
                                Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(msg_type[1]));
                                DataOutputStream Output_stream_1 = new DataOutputStream(socket_1.getOutputStream());
                                Output_stream_1.writeUTF(Update_succ_pred);
                                Output_stream_1.flush();
                                socket_1.close();

                                String Update_succ = "Update_Succ" + "~@#" + myPort + "~@#" + msg_type[1];
                                Socket socket_2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        behind);
                                DataOutputStream Output_stream_2 = new DataOutputStream(socket_2.getOutputStream());
                                Output_stream_2.writeUTF(Update_succ);
                                Output_stream_2.flush();
                                socket_2.close();
                            }
                        }
                    }

                    if (msg_type[0].equals("Initial")){
                        Succ = Integer.parseInt(msg_type[1]);
                        Pred = Integer.parseInt(msg_type[1]);
                    }

                    if (msg_type[0].equals("Update_Succ_pred")){
                        Succ = Integer.parseInt(msg_type[1]);
                        Pred = Integer.parseInt(msg_type[2]);
                    }

                    if (msg_type[0].equals("Update_Succ")){
                        Succ = Integer.parseInt(msg_type[2]);
                    }

//                    Log.v(TAG, "Current port : " + Final_port + "Successor : " + Succ + "Predecessor : " + Pred);

                    if (msg_type[0].equals("Insert")){
                        ContentValues temp_Content = new ContentValues();
                        String key = msg_type[1];
                        String value = msg_type[2];
                        String Contend = genHash(key);
                        String current = genHash(String.valueOf(Final_port/2));
                        String Succ_hash = genHash(String.valueOf(Succ/2));
                        String Pred_hash = genHash(String.valueOf(Pred/2));


                        if(current.compareTo(Pred_hash) > 0){
                            if(Contend.compareTo(current) < 0  && Contend.compareTo(Pred_hash) > 0){
//                                keyValueToInsert.put("key",key);
//                                keyValueToInsert.put("value", key_value.get(key));
                                local_key_value.put(key, value);
                            }
                            else if (Contend.compareTo(current) < 0 && Contend.compareTo(Pred_hash) < 0){
                        Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Pred);
                        String Insert_msg = "Insert" + "~@#" + key + "~@#" + value;
                        DataOutputStream Output_stream_1 = new DataOutputStream(socket_1.getOutputStream());
                        Output_stream_1.writeUTF(Insert_msg);
                        Output_stream_1.flush();
                        socket_1.close();
                            }
                            else if (Contend.compareTo(current) > 0 && Contend.compareTo(Pred_hash) > 0){
                        Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Succ);
                        String Insert_msg = "Insert" + "~@#" + key + "~@#" + value;
                        DataOutputStream Output_stream_1 = new DataOutputStream(socket_1.getOutputStream());
                        Output_stream_1.writeUTF(Insert_msg);
                        Output_stream_1.flush();
                        socket_1.close();
                            }
                        }

                        else{ //Handling edge case.
                            if(Contend.compareTo(current) > 0 && Contend.compareTo(Pred_hash) < 0){
                                Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Succ);
                                String Insert_msg = "Insert" + "~@#" + key + "~@#" + value;
                                DataOutputStream Output_stream_1 = new DataOutputStream(socket_1.getOutputStream());
                                Output_stream_1.writeUTF(Insert_msg);
                                Output_stream_1.flush();
                                socket_1.close();
                            }
                            else if((Contend.compareTo(current) > 0 && Contend.compareTo(Pred_hash) > 0) || (Contend.compareTo(current) < 0 && Contend.compareTo(Pred_hash) < 0)){
//                                keyValueToInsert.put("key",key);
//                                keyValueToInsert.put("value", key_value.get(key));
                                local_key_value.put(key, value);
                            }
                        }
                    }

//                    ObjectInputStream Query_rec = new ObjectInputStream(Server_input);
//                    QueryResults Query_recieved = (QueryResults) Query_rec.readObject();

                    if (msg_type[0].equals("Global_Dht")){
                        if(Final_port != Integer.parseInt(msg_type[1])){
//                            Log.v(TAG,"Reached inside loop" + msg_type[0] + msg_type[1] + msg_type[2]);
                            HashMap<String,String> temp_map = new HashMap<String, String>();
                            if(msg_type.length > 2) {
                                for (int i = 2; i < msg_type.length; i++) {
                                    String[] key_value = msg_type[i].split("~~");
                                    temp_map.put(key_value[0], key_value[1]);
                                }
                            }
                            for (String key : local_key_value.keySet()) {
                                temp_map.put(key, local_key_value.get(key));
                            }

                            String Init_query = "Global_Dht" + "~@#" + msg_type[1];
                            String delim_1 = "~@#";
                            String delim_2 = "~~";
                            String tempQuery = "";
                            String All_query;
                            for (String key : temp_map.keySet()){
                                tempQuery = tempQuery.concat(delim_1).concat(key).concat(delim_2).concat(temp_map.get(key));
                            }
                            All_query = Init_query + tempQuery;

//                            Log.v(TAG, All_query);
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Succ);
                            DataOutputStream Output_stream = new DataOutputStream(socket.getOutputStream());
                            Output_stream.writeUTF(All_query);
                            Output_stream.flush();
                            socket.close();
                        }
                        else{
                            HashMap<String,String> temp_map = new HashMap<String, String>();
                            for (int i = 2; i<msg_type.length; i++){
                                String[] key_value = msg_type[i].split("~~");
                                temp_map.put(key_value[0], key_value[1]);
                            }
                            Global_key_value = temp_map;
                            Global_flag = true;
                        }
                    }

                    if (msg_type[0].equals("Find_key")){
//                        Log.v(TAG,"Code Inside the condition" + msg_type[0]);
                        if(local_key_value.containsKey(msg_type[1])){
//                            Log.v(TAG,"key found");
                            String value = local_key_value.get(msg_type[1]);
                            String Find_msg = "Key_found" + "~@#" + msg_type[1] + "~@#" + value;
                            Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(msg_type[2]));
                            DataOutputStream Output_stream = new DataOutputStream(socket_1.getOutputStream());
                            Output_stream.writeUTF(Find_msg);
                            Output_stream.flush();
                            socket_1.close();
                        }
                        else{
//                            Log.v(TAG,"key forwarded to " +Succ);
                            String Find_msg = "Find_key" + "~@#" + msg_type[1] + "~@#" + msg_type[2];
                            Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Succ);
                            DataOutputStream Output_stream = new DataOutputStream(socket_1.getOutputStream());
                            Output_stream.writeUTF(Find_msg);
                            Output_stream.flush();
                            socket_1.close();
                        }
                    }

                    if (msg_type[0].equals("Key_found")){
                        key_details.put(msg_type[1], msg_type[2]);
                        key_found = true;
                    }


                    if(msg_type[0].equals("Delete_all")){
                        if(Final_port != Integer.parseInt(msg_type[1])) {
                            local_key_value.clear();
                            String all_del = "Delete_all" + "~@#" + msg_type[1];
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Succ);
                            DataOutputStream Output_stream = new DataOutputStream(socket.getOutputStream());
                            Output_stream.writeUTF(all_del);
                            Output_stream.flush();
                            socket.close();
                        }
                    }

                    if(msg_type[0].equals("Delete_key")){
                        if(local_key_value.containsKey(msg_type[1])){
//                            Log.v(TAG,"key found");
                            local_key_value.remove(msg_type[1]);
                        }
                        else{
//                            Log.v(TAG,"key forwarded to " +Succ);
                            String del_msg = "Delete_key" + "~@#" + msg_type[1] + "~@#" + msg_type[2];
                            Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Succ);
                            DataOutputStream Output_stream = new DataOutputStream(socket_1.getOutputStream());
                            Output_stream.writeUTF(del_msg);
                            Output_stream.flush();
                            socket_1.close();
                        }

                    }



                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e){
                e.printStackTrace();
            }

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            return null;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String msg_recieved = msgs[0];
            String portFrom = msgs[1];

            if(msg_recieved.equals("Node_Join")) {
                try {


                    String remotePort = "11108";
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));

                    String Node_join_msg = "Node_Join" + "~@#" + portFrom;
//                    Log.v(TAG, Node_join_msg);
//                    wait(2);
                    DataOutputStream Output_stream = new DataOutputStream(socket.getOutputStream());
                    Output_stream.writeUTF(Node_join_msg);
                    Output_stream.flush();
//                    socket.wait(500);
                    socket.close();
//                    Log.v(TAG, "Msg sent :" + Node_join_msg);

                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (msg_recieved.equals("Handle_Insert_Succ")){
                try {

                    String remotePort = String.valueOf(Succ);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));

                    String Insert_msg = "Insert" + "~@#" + msgs[2] + "~@#" + msgs[3];
                    DataOutputStream Output_stream = new DataOutputStream(socket.getOutputStream());
                    Output_stream.writeUTF(Insert_msg);
                    Output_stream.flush();
                    socket.close();
//                    Log.v(TAG, "Msg sent :" + Insert_msg);

                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (msg_recieved.equals("Handle_Insert_Pred")){
                try {

                    String remotePort = String.valueOf(Pred);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));

                    String Insert_msg = "Insert" + "~@#" + msgs[2] + "~@#" + msgs[3];
                    DataOutputStream Output_stream = new DataOutputStream(socket.getOutputStream());
                    Output_stream.writeUTF(Insert_msg);
                    Output_stream.flush();
                    socket.close();
//                    Log.v(TAG, "Msg sent :" + Insert_msg);

                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


            return null;
        }
    }






    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        //https://developer.android.com/training/basics/data-storage/databases.html
//        Cursor query_result;
//        db_help mDbHelper = new db_help(getContext());
//        SQLiteDatabase db = mDbHelper.getReadableDatabase();
//        String column_to_query = "key";
//        String selection_final = column_to_query + " = ?";
//        String[] selection_filter = { selection };
//
//        query_result = db.query(table_name,null,selection_final,selection_filter,null,null,sortOrder);
//
//        query_result.moveToFirst();
        String[] column_name = {"key", "value"};
        MatrixCursor query_result = new MatrixCursor(column_name);


        if (selection.equals("@")){
            for (String key : local_key_value.keySet()) {
                String[] row = {key, local_key_value.get(key)};
                query_result.addRow(row);
            }
        }

        else if (selection.equals("*")){
            if (Final_port == Succ && Final_port == Pred){
                for (String key : local_key_value.keySet()) {
                    String[] row = {key, local_key_value.get(key)};
                    query_result.addRow(row);
                }
            }
            else {
                String Init_query = "Global_Dht" + "~@#" + Final_port;
                String delim_1 = "~@#";
                String delim_2 = "~~";
                String tempQuery = "";
                String All_query;
//                Log.v(TAG, String.valueOf(local_key_value.size()));
                for (String key : local_key_value.keySet()){
//                    All_query = delim_1 + key + delim_2 + local_key_value.get(key);
                    tempQuery = tempQuery.concat(delim_1).concat(key).concat(delim_2).concat(local_key_value.get(key));
                }
                try {
                    All_query = Init_query + tempQuery;
//                    Log.v(TAG, All_query);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Succ);
                    DataOutputStream Output_stream = new DataOutputStream(socket.getOutputStream());
                    Output_stream.writeUTF(All_query);
                    Output_stream.flush();
                    socket.close();

                    while (true) {
                        if (Global_flag == true) {
                            Global_flag = false;
                            break;
                        }
                    }

                    for (String key : Global_key_value.keySet()) {
                        String[] row = {key, Global_key_value.get(key)};
                        query_result.addRow(row);
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        else{
            if(local_key_value.containsKey(selection)){
                String[] row = {selection, local_key_value.get(selection)};
                query_result.addRow(row);
//                Log.v(TAG, selection + " was not found here");
            }
            else{
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Succ);

                    String Find_msg = "Find_key" + "~@#" + selection + "~@#" + Final_port;
                    DataOutputStream Output_stream = new DataOutputStream(socket.getOutputStream());
                    Output_stream.writeUTF(Find_msg);
                    Output_stream.flush();
//                    Log.v(TAG, selection + "sent to " + Succ);
                    socket.close();

                    while (true) {
                        if (key_found == true) {
                            String[] row = {selection, key_details.get(selection)};
                            query_result.addRow(row);
                            key_details.remove(selection);
                            key_found = false;
                            break;
                        }
                    }
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        Log.v("query", selection);
        return query_result;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
