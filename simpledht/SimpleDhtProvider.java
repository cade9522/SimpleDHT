package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

/***
 * The SimpleDhtProvider class implements a simple Distributed Hash Table
 * based on Chord. The class extends the Android ContentProvider for data
 * manipulation implementing the "insert", "delete", and "query" functions.
 *
 * @author: caevans
 */
public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String[] PORTS = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    private String _successor, _predecessor, _local;
    private String _hashS, _hashP, _hashL;
    private String _portS, _portP, _portL;
    private Uri _uri;

    /* The delete method checks for a given key and deletes the entry.
     * Using the provided key, a SHA-1 hash is created and checked against
     * the hash of the AVD port (5554 ... 5562) to see if the entry resides
     * on the current device. If so, it deletes it. If not, the hash is compared
     * to hashes of the successor and predecessor nodes of the ring. A message is
     * then sent accordingly to whichever node to call delete on the same key on
     * that device. The special characters '@' and '*' will delete all entries
     * on a given node and all entries across the entire ring, respectively.
     */
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        String key = selection;

        if(key.equals("@")) {
            return remove(key);
        }
        if(key.equals("*")) {
            if(_successor == null) {
                return remove(key);
            }
            else {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "delete", _portL, _portP, "*", "---");
                return 1;
            }
        }

        String hash = null;
        try {
            hash = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        int comp_local = hash.compareTo(_hashL);
        if(comp_local < 0) {
            if(_predecessor == null) {
               return remove(key);
            }
            else {
                int comp_pred = hash.compareTo(_hashP);
                if(comp_pred > 0) {
                    return remove(key);
                }
                else {
                    int comp_nodes = _hashP.compareTo(_hashL);
                    if(comp_nodes < 0) {
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "delete", _portL, _portP, key, "---");
                        return 2;
                    }
                    else {
                        return remove(key);
                    }
                }
            }
        }
        else {
            if(_successor == null) {
                return remove(key);
            }
            else {
                int comp_nodes = _hashP.compareTo(_hashL);
                int comp_pred  = hash.compareTo(_hashP);
                if((comp_nodes > 0) && (comp_pred > 0)) {
                    return remove(key);
                }
                else {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "delete", _portL, _portS, key, "---");
                    return 2;
                }
            }
        }
    }

    // Method not defined
    @Override
    public String getType(Uri uri) {
        return null;
    }

    /* The insert method functions similarly to delete() however checks to see if
     * a given key should be stored on the current device. If not, a message is sent
     * forward or back on the ring to insert the key.
     */
    @Override
    public Uri insert(Uri uri, ContentValues values) {

        String key = values.getAsString("key");
        String value = values.getAsString("value");

        String hash = null;
        try {
            hash = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        int comp_local = hash.compareTo(_hashL);
        if(comp_local < 0) {
            if(_predecessor == null) {
                store(key, value);
            }
            else {
                int comp_pred = hash.compareTo(_hashP);
                if(comp_pred > 0) {
                    store(key, value);
                }
                else {
                    int comp_nodes = _hashP.compareTo(_hashL);
                    if(comp_nodes < 0) {
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", _portL, _portP, key, value);
                    }
                    else {
                        store(key, value);
                    }
                }
            }
        }
        else {
            if(_successor == null) {
                store(key, value);
            }
            else {
                int comp_nodes = _hashP.compareTo(_hashL);
                int comp_pred  = hash.compareTo(_hashP);
                if((comp_nodes > 0) && (comp_pred > 0)) {
                    store(key, value);
                }
                else {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", _portL, _portS, key, value);
                }
            }
        }

        return uri;
    }

    /* onCreate is called initially as the ContentProvider is created. All private
     * variables are initialized accordingly and a join message is sent to to AVD
     * 5554 (if not the current AVD) to see if a network of rings already exists
     */
    @Override
    public boolean onCreate() {

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        _portL = String.valueOf((Integer.parseInt(portStr) * 2));
        _local = portStr;
        try {
            _hashL = genHash(_local);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Local Hash Error");
        }
        Log.v(TAG, "node " + _local + " hash is " + _hashL);
        _uri = build();
        _successor = null;
        _predecessor = null;
        _hashP = null;
        _hashS = null;

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return true;
        }

        if(!_local.equals("5554")) {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "join", _portL, PORTS[0], _local, "---");
        }

        return true;
    }

    /* The query method operates just as delete() and insert() for key checks
     * but unlike those two must wait for a response (if the query is sent to
     * another AVD) for a given key. The special characters '@' and '*' will
     * return all entries for a given node and all entries across the entire
     * ring, respectively.
     */
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {

        String key = selection;
        String hash = null;

        if(key.equals("@")) {
            return get(key);
        }
        if(key.equals("*")) {
            if(_successor == null) {
                return get(key);
            }
            else {
                if(_successor.equals(sortOrder)) {
                    return get(key);
                }
                else {
                    if(sortOrder == null) {
                        sortOrder = _local;
                    }
                    Socket socket = send(null, "query", _portL, _portS, key, sortOrder);

                    while(true) {
                        try {
                            DataInputStream receive = new DataInputStream(socket.getInputStream());
                            String[] reception = receive.readUTF().split(":::");
                            String op = reception[0];
                            String kee = reception[3];
                            String value = reception[4];

                            if (kee.equals("...")) {
                                return get(key);
                            } else if (op.equals("query_resp")) {
                                String[] columns = {"key", "value"};
                                MatrixCursor cursor = new MatrixCursor(columns);
                                String[] kee2 = kee.split("~~~");
                                String[] value2 = value.split("~~~");
                                for (int i = 0; i < kee2.length; i++) {
                                    String[] row = {kee2[i], value2[i]};
                                    cursor.addRow(row);
                                }
                                Cursor cursor1 = get(key);
                                if (cursor1.moveToFirst()) {
                                    do {
                                        String kee3 = cursor1.getString(cursor1.getColumnIndex("key"));
                                        String value3 = cursor1.getString(cursor1.getColumnIndex("value"));
                                        String[] row = {kee3, value3};
                                        cursor.addRow(row);
                                    } while (cursor1.moveToNext());
                                }
                                return cursor;
                            }

                        } catch (IOException e) {
                            Log.e(TAG, "Read exception");
                        }
                    }
                }
            }
        }

        try {
            hash = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        int comp_local = hash.compareTo(_hashL);
        if(comp_local < 0) {
            if(_predecessor == null) {
                return get(key);
            }
            else {
                int comp_pred = hash.compareTo(_hashP);
                if(comp_pred > 0) {
                    return get(key);
                }
                else {
                    int comp_nodes = _hashP.compareTo(_hashL);
                    if(comp_nodes < 0) {
                        Socket socket = send(null, "query", _portL, _portP, key, "---");

                        while(true) {
                            try {
                                DataInputStream receive = new DataInputStream(socket.getInputStream());
                                String[] reception = receive.readUTF().split(":::");
                                String op = reception[0];
                                String value = reception[4];

                                if (key.equals("...")) {
                                    return null;
                                }

                                if (op.equals("query_resp")) {
                                    String[] columns = {"key", "value"};
                                    MatrixCursor cursor = new MatrixCursor(columns);
                                    String[] row = {key, value};
                                    cursor.addRow(row);
                                    return cursor;
                                }

                            } catch (IOException e) {
                                Log.e(TAG, "Read exception");
                            }
                        }

                    }
                    else {
                        return get(key);
                    }
                }
            }
        }
        else {
            if(_successor == null) {
                return get(key);
            }
            else {
                int comp_nodes = _hashP.compareTo(_hashL);
                int comp_pred  = hash.compareTo(_hashP);
                if((comp_nodes > 0) && (comp_pred > 0)) {
                    return get(key);
                }
                else {
                    Socket socket = send(null, "query", _portL, _portS, key, "---");

                    while (true) {
                        try {
                            DataInputStream receive = new DataInputStream(socket.getInputStream());
                            String[] reception = receive.readUTF().split(":::");
                            String op = reception[0];
                            String value = reception[4];

                            if (key.equals("...")) {
                                return null;
                            }

                            if (op.equals("query_resp")) {
                                String[] columns = {"key", "value"};
                                MatrixCursor cursor = new MatrixCursor(columns);
                                String[] row = {key, value};
                                cursor.addRow(row);
                                return cursor;
                            }

                        } catch (IOException e) {
                            Log.e(TAG, "Read exception");
                        }

                        return null;
                    }
                }
            }
        }
    }

    // Method not defined
    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }

    /* Method to send a string from one port to another via a TCP socket. A socket
     * is created if one is not provided as a parameter. Every other parameter is
     * combined to a single string and sent via TCP using UTF-8 encoding. The used
     * socket is returned
     */
    private Socket send(Socket socket, String... all) {

        String op = all[0];
        String localPort = all[1];
        String remotePort = all[2];
        String key = all[3];
        String value = all[4];

        try {
            if (socket == null) {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));
            }

            String msgToSend = op + ":::" + localPort + ":::" + remotePort + ":::" + key + ":::" + value;
            DataOutputStream send = new DataOutputStream(socket.getOutputStream());
            send.writeUTF(msgToSend);
            send.flush();
            //socket.close();

        } catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException");
        } catch (IOException e) {
            Log.e(TAG, "ClientTask socket IOException");
        }

        return socket;
    }

    /* The store method is a helper function for insert(). Given a key and
     * value, an entry is made and stored on the device.
     */
    private void store(String key, String value) {

        getContext().deleteFile(key);

        FileOutputStream outputStream;
        try {
            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            Log.v(TAG, "storing " + key + " - " + value + " on " + _local);
            outputStream.close();
        } catch (Exception e) {
            Log.e(TAG, "File write failed");
        }

    }

    /* A helper function for delete(). For a given key, a storage entry is
     * deleted. If the key is either '@' or '*', all local entries are erased.
     */
    private int remove(String key) {

        if(key.equals("@") || key.equals("*")) {
            String[] list = getContext().fileList();
            for(int i = 0; i < list.length; i++) {
                getContext().deleteFile(list[0]);
            }
        }
        else {
            try {
                getContext().openFileInput(key);
                getContext().deleteFile(key);

            } catch (FileNotFoundException e) {
                Log.e(TAG, "No file to delete");
                return 0;
            }
        }

        return 1;
    }

    /* The local query helper function to return a stored value (or all values,
     * provided the key is either '@' or '*') on the device. The key-value pair
     * is returned as a MatrixCursor.
     */
    private Cursor get(String key) {

        String[] columns = {"key", "value"};
        MatrixCursor cursor = new MatrixCursor(columns);

        String value;
        FileInputStream inputStream;

        if(key.equals("@") || key.equals("*")) {
            Log.v(TAG, "STARTING FULL QUERY ON NODE " + _local);
            String[] list = getContext().fileList();
            for(int i = 0; i < list.length; i++) {
                try {
                    inputStream = getContext().openFileInput(list[i]);
                    BufferedReader input = new BufferedReader(new InputStreamReader(inputStream));
                    value = input.readLine();

                    String[] row= {list[i], value};
                    cursor.addRow(row);

                    inputStream.close();
                } catch (Exception e) {
                    Log.e(TAG, "File write failed");
                }
            }
        }
        else {
            try {
                inputStream = getContext().openFileInput(key);
                BufferedReader input = new BufferedReader(new InputStreamReader(inputStream));
                value = input.readLine();

                String[] row= {key, value};
                cursor.addRow(row);

                inputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }
        }

        MatrixCursor backup = cursor;
        if (cursor.moveToFirst()){
            do{
                String k = cursor.getString(cursor.getColumnIndex("key"));
                String v = cursor.getString(cursor.getColumnIndex("value"));
                Log.v(TAG, "querying " + k + " --- " + v);
            }while(cursor.moveToNext());
        }

        cursor = backup;
        return cursor;
    }

    // Helper method to set the correct Uri
    private Uri build(){
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.scheme("content");
        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
        return uriBuilder.build();
    }

    // Used to generate a SHA-1 hash of a given string
    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    /***
     * ServerTask is an AsyncTask to handle incoming messages. All functionality
     * is performed in the background on a seperte thread to keep the main program
     * functioning. A ServerSocket is used to fetch TCP messages (and continuously
     * loop through looking for them) from other AVDs. Once a message is found,
     * it is broken apart and the sent operation is performed (between insert,
     * delete, query, join, and join_resp).
     */
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            Socket socket = null;

            do {
                try {

                    socket = serverSocket.accept();
                    DataInputStream receive = new DataInputStream(socket.getInputStream());

                    String[] reception = receive.readUTF().split(":::");
                    String op = reception[0];
                    String remotePort = reception[1];
                    String localPort = reception[2];
                    String key = reception[3];
                    String value = reception[4];

                    // Insert and delete will have a node perform a local call on either function
                    if(op.equals("insert")) {
                        ContentValues cv = new ContentValues();
                        cv.put("key", key);
                        cv.put("value", value);
                        insert(_uri, cv);
                    }
                    else if(op.equals("delete")) {
                        delete(_uri, key, null);
                    }

                    /* If a query call is received, the local query() is called and
                     * result is broken apart and sent via string message back to
                     * the sender
                     */
                    else if(op.equals("query")) {
                        Cursor cursor = query(_uri, null, key, null, value);
                        if(cursor.moveToFirst()) {
                            String val = cursor.getString(cursor.getColumnIndex("value"));
                            String kee = cursor.getString(cursor.getColumnIndex("key"));
                            if (key.equals("*")) {
                                while (cursor.moveToNext()) {
                                    String kee1 = cursor.getString(cursor.getColumnIndex("key"));
                                    kee = kee + "~~~" + kee1;
                                    String val1 = cursor.getString(cursor.getColumnIndex("value"));
                                    val = val + "~~~" + val1;
                                }
                                send(socket, "query_resp", localPort, remotePort, kee, val);
                            } else {
                                val = cursor.getString(cursor.getColumnIndex("value"));
                                send(socket, "query_resp", localPort, remotePort, key, val);
                            }
                        }
                        else {
                            send(socket, "query_resp", localPort, remotePort, "...", "...");
                        }
                    }

                    /* A join operation checks the local successor and predecessor
                     * hashes to determine a joining nodes position on the ring.
                     * If the node doesn't fit as a predecessor or successor to the
                     * current AVD, a join message is sent forward or back on the
                     * ring accordingly. If the predecessor node is changed, all
                     * stored values are queried and reinserted to the ring to even
                     * out and correct storage to new nodes.
                     */
                    else if(op.equals("join")) {
                        boolean resort = false;
                        if(_predecessor == null) {
                            resort = true;
                            _predecessor = key;
                            _successor = key;
                            Log.v(TAG, "new nodes are " + _local + " : " + _predecessor + " - " + _successor);
                            _portP = remotePort;
                            _portS = remotePort;
                            send(null, "join_resp", localPort, remotePort, _local, _local);
                        }
                        else {
                            String hash = null;
                            try {
                                hash = genHash(key);
                            } catch (NoSuchAlgorithmException e) {
                                Log.e(TAG, "Can't hash node");
                            }
                            int comp = hash.compareTo(_hashL);
                            int comp_local = _hashL.compareTo(_hashS);
                            int comp_local2 = _hashL.compareTo(_hashP);
                            int comp_succ = hash.compareTo(_hashS);
                            int comp_pred = hash.compareTo(_hashP);

                            if(comp > 0) {
                                if((comp_local > 0) || (comp_succ < 0)) {
                                    send(null, "join_resp", localPort, remotePort, _local, _successor);
                                    send(null, "join_resp", localPort, _portS, key, "---");

                                    _successor = key;
                                    _portS = String.valueOf(Integer.parseInt(_successor) * 2);
                                    Log.v(TAG, "new nodes are " + _local + " : " + _predecessor + " - " + _successor);
                                }
                                else {
                                    send(null, "join", remotePort, _portS, key, "---");
                                }
                            }
                            else {
                                if((comp_local2 < 0) || (comp_pred > 0)) {
                                    send(null, "join_resp", localPort, remotePort, _predecessor, _local);
                                    send(null, "join_resp", localPort, _portP, "---", key);

                                    resort = true;
                                    _predecessor = key;
                                    _portP = String.valueOf(Integer.parseInt(_predecessor) * 2);
                                    Log.v(TAG, "new nodes are " + _local + " : " + _predecessor + " - " + _successor);
                                }
                                else {
                                    send(null, "join", remotePort, _portP, key, "---");
                                }
                            }
                        }

                        try {
                            _hashP = genHash(_predecessor);
                            _hashS = genHash(_successor);
                        } catch (NoSuchAlgorithmException e) {
                            Log.e(TAG, "Can't hash node");
                        }

                        if(resort) {
                            Cursor cursor = query(_uri, null, "@", null, null);
                            delete(_uri, "@", null);
                            if(cursor.moveToFirst()) {
                                do {
                                    String val = cursor.getString(cursor.getColumnIndex("value"));
                                    String kee = cursor.getString(cursor.getColumnIndex("key"));
                                    ContentValues cv = new ContentValues();
                                    cv.put("key", kee);
                                    cv.put("value", val);
                                    insert(_uri, cv);
                                } while (cursor.moveToNext());
                            }
                        }

                    }

                    /* A determined position from a newly joined node will send a response
                     * to effected nodes to update their own pointers. If the predecessor
                     * node is changed, all stored values are queried and reinserted to the
                     * ring to even out and correct storage to new nodes.
                     */
                    else if(op.equals("join_resp")) {
                        boolean resort = false;
                        if(!key.equals("---")) {
                            resort = true;
                            _predecessor = key;
                            _portP = String.valueOf(Integer.parseInt(_predecessor) * 2);
                        }
                        if(!value.equals("---")) {
                            _successor = value;
                            _portS = String.valueOf(Integer.parseInt(_successor) * 2);
                        }

                        Log.v(TAG, "new nodes are " + _local + " : " + _predecessor + " - " + _successor);

                        try {
                            _hashP = genHash(_predecessor);
                            _hashS = genHash(_successor);
                        } catch (NoSuchAlgorithmException e) {
                            Log.e(TAG, "Can't hash node");
                        }

                        if(resort) {
                            Cursor cursor = query(_uri, null, "@", null, null);
                            delete(_uri, "@", null);
                            if(cursor.moveToFirst()) {
                                do {
                                    String val = cursor.getString(cursor.getColumnIndex("value"));
                                    String kee = cursor.getString(cursor.getColumnIndex("key"));
                                    ContentValues cv = new ContentValues();
                                    cv.put("key", kee);
                                    cv.put("value", val);
                                    insert(_uri, cv);
                                } while (cursor.moveToNext());
                            }
                        }

                    }

                } catch (IOException e) {
                    Log.e(TAG, "ServerSocket IOException");
                }

            } while(!socket.isInputShutdown());

            return null;

        }

    }


    /***
     * ClientTask is an AsyncTask that sends a message to a remote port.
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            // Declare message parameters
            String op = msgs[0];
            String localPort = msgs[1];
            String remotePort = msgs[2];
            String key = msgs[3];
            String value = msgs[4];

            Log.v(TAG, "sending " + key + " from " + _local + " to " + remotePort);
            send(null, op, localPort, remotePort, key, value);
            return null;
        }
    }

}
