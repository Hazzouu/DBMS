package DBMS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class BitmapIndex implements Serializable {
    private String columnName;
    private Map<String, ArrayList<Boolean>> bitmaps;
    private int totalRecords;

    public BitmapIndex(String columnName) {
        this.columnName = columnName;
        this.bitmaps = new HashMap<>();
        this.totalRecords = 0;
    }

    public void insert(String value) {
        // Initialize bitmap for new value if it doesn't exist
        if (!bitmaps.containsKey(value)) {
            ArrayList<Boolean> bitmap = new ArrayList<>();
            for (int i = 0; i < totalRecords; i++) {
                bitmap.add(false);
            }
            bitmaps.put(value, bitmap);
        }

        // Update all bitmaps
        for (Map.Entry<String, ArrayList<Boolean>> entry : bitmaps.entrySet()) {
            ArrayList<Boolean> bitmap = entry.getValue();
            if (entry.getKey().equals(value)) {
                bitmap.add(true);
            } else {
                bitmap.add(false);
            }
        }
        totalRecords++;
    }

    public ArrayList<Integer> select(String value) {
        ArrayList<Integer> result = new ArrayList<>();
        ArrayList<Boolean> bitmap = bitmaps.get(value);
        
        if (bitmap != null) {
            for (int i = 0; i < bitmap.size(); i++) {
                if (bitmap.get(i)) {
                    result.add(i);
                }
            }
        }
        return result;
    }

    public String getColumnName() {
        return columnName;
    }

    public int getTotalRecords() {
        return totalRecords;
    }

    public Map<String, ArrayList<Boolean>> getBitmaps() {
        return bitmaps;
    }
} 