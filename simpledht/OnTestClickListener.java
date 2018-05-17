package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;

public class OnTestClickListener implements OnClickListener {

	private static final String TAG = OnTestClickListener.class.getName();
	private static final int TEST_CNT = 50;
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";

	private final TextView mTextView;
	private final ContentResolver mContentResolver;
	private final Uri mUri;
	private final ContentValues[] mContentValues;
	private final String _port;
	private int _counter;

	public OnTestClickListener(TextView _tv, ContentResolver _cr, String port) {
		mTextView = _tv;
		mContentResolver = _cr;
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
		mContentValues = initTestValues();
		_port = port;
		_counter = 0;
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	private ContentValues[] initTestValues() {
		ContentValues[] cv = new ContentValues[TEST_CNT];
		for (int i = 0; i < TEST_CNT; i++) {
			cv[i] = new ContentValues();
			cv[i].put(KEY_FIELD, "key" + Integer.toString(i));
			cv[i].put(VALUE_FIELD, "val" + Integer.toString(i));
		}

		return cv;
	}

	@Override
	public void onClick(View v) {
		new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
	}

	private class Task extends AsyncTask<Void, String, Void> {

		@Override
		protected Void doInBackground(Void... params) {

			if (testInsert()) {
				publishProgress("Insert success\n");
			} else {
				publishProgress("Insert fail\n");
				return null;
			}

			if (testQuery()) {
			    publishProgress("Query success\n");
			} else {
                publishProgress("Query fail\n");
            }

            /*
			if (_counter == 0) {
				_counter++;
				test1();
			} else if(_counter == 1){
				_counter++;
				test2();
			}
			else {
				test3();
			}
			*/

			return null;
		}

		protected void onProgressUpdate(String... strings) {
			mTextView.append(strings[0]);

			return;
		}

		private boolean testInsert() {
			try {
				for (int i = 0; i < TEST_CNT; i++) {
					mContentResolver.insert(mUri, mContentValues[i]);
				}
			} catch (Exception e) {
				Log.e(TAG, e.toString());
				return false;
			}

			return true;
		}

		private boolean testQuery() {

			try {
				for (int i = 0; i < TEST_CNT; i++) {
					String key = (String) mContentValues[i].get(KEY_FIELD);
					String val = (String) mContentValues[i].get(VALUE_FIELD);

					Cursor resultCursor = mContentResolver.query(mUri, null,
							key, null, null);
					if (resultCursor == null) {
						Log.e(TAG, "Result null");
						throw new Exception();
					}

					int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
					int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
					if (keyIndex == -1 || valueIndex == -1) {
						Log.e(TAG, "Wrong columns");
						resultCursor.close();
						throw new Exception();
					}

					resultCursor.moveToFirst();

					if (!(resultCursor.isFirst() && resultCursor.isLast())) {
						Log.e(TAG, "Wrong number of rows");
						resultCursor.close();
						throw new Exception();
					}

					String returnKey = resultCursor.getString(keyIndex);
					String returnValue = resultCursor.getString(valueIndex);
					if (!(returnKey.equals(key) && returnValue.equals(val))) {
						Log.e(TAG, "(key, value) pairs don't match\n");
						resultCursor.close();
						throw new Exception();
					}

					resultCursor.close();
				}
			} catch (Exception e) {
				return false;
			}

			return true;
		}

		private boolean test1() {
			if (_port.equals("11108")) {
				ContentValues cv = new ContentValues();
				cv.put("key", "one");
				cv.put("value", "two");
				mContentResolver.insert(mUri, cv);
				ContentValues cv1 = new ContentValues();
				cv.put("key", "one1");
				cv.put("value", "two1");
				mContentResolver.insert(mUri, cv);
				ContentValues cv2 = new ContentValues();
				cv.put("key", "one2");
				cv.put("value", "two2");
				mContentResolver.insert(mUri, cv);
				ContentValues cv3 = new ContentValues();
				cv.put("key", "one3");
				cv.put("value", "two3");
				mContentResolver.insert(mUri, cv);
				ContentValues cv4 = new ContentValues();
				cv.put("key", "one4");
				cv.put("value", "two4");
				mContentResolver.insert(mUri, cv);
			} else {
				ContentValues cv = new ContentValues();
				cv.put("key", "three");
				cv.put("value", "four");
				mContentResolver.insert(mUri, cv);
				ContentValues cv1 = new ContentValues();
				cv.put("key", "three1");
				cv.put("value", "four1");
				mContentResolver.insert(mUri, cv);
				ContentValues cv2 = new ContentValues();
				cv.put("key", "three2");
				cv.put("value", "four2");
				mContentResolver.insert(mUri, cv);
				ContentValues cv3 = new ContentValues();
				cv.put("key", "three3");
				cv.put("value", "four3");
				mContentResolver.insert(mUri, cv);
				ContentValues cv4 = new ContentValues();
				cv.put("key", "three4");
				cv.put("value", "four4");
				mContentResolver.insert(mUri, cv);
			}
			return true;
		}

		private boolean test2() {
			Cursor resultCursor= mContentResolver.query(mUri, null, "@", null, null);
			if (resultCursor.moveToFirst()) {
				do {
					String kee3 = resultCursor.getString(resultCursor.getColumnIndex("key"));
					String value3 = resultCursor.getString(resultCursor.getColumnIndex("value"));
					Log.v(TAG, kee3 + " - " + value3);
				} while (resultCursor.moveToNext());
			}
			return true;
		}

		private boolean test3() {
			Cursor resultCursor= mContentResolver.query(mUri, null, "*", null, null);
			if (resultCursor.moveToFirst()) {
				do {
					String kee3 = resultCursor.getString(resultCursor.getColumnIndex("key"));
					String value3 = resultCursor.getString(resultCursor.getColumnIndex("value"));
					Log.v(TAG, kee3 + " - " + value3);
				} while (resultCursor.moveToNext());
			}
			return true;
		}
	}
}
