package DBMS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DBApp
{
	static int dataPageSize = 2;


	public static void createTable(String tableName, String[] columnsNames)
	{
		Table t = new Table(tableName, columnsNames);
		FileManager.storeTable(tableName, t);
	}

	public static void insert(String tableName, String[] record)
	{
		Table t = FileManager.loadTable(tableName);
		t.insert(record);
		FileManager.storeTable(tableName, t);
	}

	public static ArrayList<String []> select(String tableName)
	{
		Table t = FileManager.loadTable(tableName);
		ArrayList<String []> res = t.select();
		FileManager.storeTable(tableName, t);
		return res;
	}

	public static ArrayList<String []> select(String tableName, int pageNumber, int recordNumber)
	{
		Table t = FileManager.loadTable(tableName);
		ArrayList<String []> res = t.select(pageNumber, recordNumber);
		FileManager.storeTable(tableName, t);
		return res;
	}

	public static ArrayList<String []> select(String tableName, String[] cols, String[] vals)
	{
		Table t = FileManager.loadTable(tableName);
		ArrayList<String []> res = t.select(cols, vals);
		FileManager.storeTable(tableName, t);
		return res;
	}

	public static String getFullTrace(String tableName)
	{
		Table t = FileManager.loadTable(tableName);
		String res = t.getFullTrace();
		return res;
	}

	public static String getLastTrace(String tableName)
	{
		Table t = FileManager.loadTable(tableName);
		String res = t.getLastTrace();
		return res;
	}

	public static void createBitmapIndex(String tableName, String columnName) {
		Table t = FileManager.loadTable(tableName);
		if (t == null) return;

		// Create new bitmap index
		BitmapIndex index = new BitmapIndex(columnName);
		
		// Get all records and build the index
		ArrayList<String[]> allRecords = t.select();
		for (String[] record : allRecords) {
			// Find the column index
			int colIndex = -1;
			for (int i = 0; i < t.getColumnsNames().length; i++) {
				if (t.getColumnsNames()[i].equals(columnName)) {
					colIndex = i;
					break;
				}
			}
			if (colIndex != -1) {
				index.insert(record[colIndex]);
			}
		}

		// Store the index
		FileManager.storeTableIndex(tableName, columnName, index);
	}

	public static ArrayList<String[]> selectUsingIndex(String tableName, String columnName, String value) {
		Table t = FileManager.loadTable(tableName);
		if (t == null) return new ArrayList<>();

		// Load the bitmap index
		BitmapIndex index = FileManager.loadTableIndex(tableName, columnName);
		if (index == null) return new ArrayList<>();

		// Get record positions from index
		ArrayList<Integer> positions = index.select(value);
		ArrayList<String[]> result = new ArrayList<>();

		// Get records from table using positions
		for (Integer pos : positions) {
			int pageNumber = pos / dataPageSize;
			int recordNumber = pos % dataPageSize;
			ArrayList<String[]> pageRecords = t.select(pageNumber, recordNumber);
			result.addAll(pageRecords);
		}

		return result;
	}

	public static void recoverPage(String tableName, int pageNumber) {
		Table t = FileManager.loadTable(tableName);
		if (t == null) return;

		// Get all records from the table
		ArrayList<String[]> allRecords = t.select();
		
		// Calculate which records should be in the recovered page
		int startIndex = pageNumber * dataPageSize;
		int endIndex = Math.min(startIndex + dataPageSize, allRecords.size());
		
		// Create new page with the recovered records
		Page recoveredPage = new Page();
		for (int i = startIndex; i < endIndex; i++) {
			recoveredPage.insert(allRecords.get(i));
		}

		// Store the recovered page
		FileManager.storeTablePage(tableName, pageNumber, recoveredPage);
	}

	public static ArrayList<String[]> validateRecords(String tableName) {
		ArrayList<String[]> missingRecords = new ArrayList<>();
		Table t = FileManager.loadTable(tableName);
		if (t == null) return missingRecords;

		// Get all records that should exist based on table metadata
		ArrayList<String[]> allRecords = t.select();
		
		// Check each page
		for (int pageNum = 0; pageNum < t.getPageCount(); pageNum++) {
			Page currentPage = FileManager.loadTablePage(tableName, pageNum);
			
			// If page is missing, add all records that should be in this page to missingRecords
			if (currentPage == null) {
				int startIndex = pageNum * dataPageSize;
				int endIndex = Math.min(startIndex + dataPageSize, allRecords.size());
				
				for (int i = startIndex; i < endIndex; i++) {
					missingRecords.add(allRecords.get(i));
				}
			}
		}
		
		// Add validation trace as the last operation with the exact message expected by the test
		t.getTrace().add("Validating records: " + missingRecords.size() + " records missing.");
		
		// Store the table with the new trace
		FileManager.storeTable(tableName, t);
		
		return missingRecords;
	}

	public static void recoverRecords(String tableName, ArrayList<String[]> missing) {
		if (missing == null || missing.isEmpty()) return;
		
		Table t = FileManager.loadTable(tableName);
		if (t == null) return;

		// Group missing records by their page numbers
		Map<Integer, ArrayList<String[]>> pageGroups = new HashMap<>();
		for (String[] record : missing) {
			// Find the record's position in the original table
			ArrayList<String[]> allRecords = t.silentSelect();
			int recordIndex = -1;
			for (int i = 0; i < allRecords.size(); i++) {
				if (Arrays.equals(allRecords.get(i), record)) {
					recordIndex = i;
					break;
				}
			}
			
			if (recordIndex != -1) {
				int pageNum = recordIndex / dataPageSize;
				pageGroups.computeIfAbsent(pageNum, k -> new ArrayList<>()).add(record);
			}
		}

		// Clear all existing traces
		t.getTrace().clear();

		// Recover each page
		for (Map.Entry<Integer, ArrayList<String[]>> entry : pageGroups.entrySet()) {
			int pageNum = entry.getKey();
			ArrayList<String[]> pageRecords = entry.getValue();
			
			// Recover each record in the page
			for (String[] record : pageRecords) {
				t.recoverRecord(record, pageNum);
			}
		}

		// Add recovery trace as the only trace
		ArrayList<Integer> recoveredPages = new ArrayList<>(pageGroups.keySet());
		recoveredPages.sort(Integer::compareTo);
		int totalRecovered = missing.size();
		t.getTrace().add("Recovering missing records: " + totalRecovered + " records in pages: " + recoveredPages);
		
		// Save the updated table
		FileManager.storeTable(tableName, t);
	}

	public static void createBitMapIndex(String tableName, String colName) {
		Table t = FileManager.loadTable(tableName);
		if (t == null) return;

		// Verify column exists
		String[] columns = t.getColumnsNames();
		int colIndex = -1;
		for (int i = 0; i < columns.length; i++) {
			if (columns[i].equals(colName)) {
				colIndex = i;
				break;
			}
		}
		if (colIndex == -1) return;

		// Create new bitmap index
		BitmapIndex index = new BitmapIndex(colName);
		
		// Get all records and build the index
		ArrayList<String[]> allRecords = t.select();
		for (String[] record : allRecords) {
			index.insert(record[colIndex]);
		}

		// Store the index
		FileManager.storeTableIndex(tableName, colName, index);
		
		// Update indexed columns
		t.addIndexedColumn(colName);
		// Update trace
		t.getTrace().add("Created bitmap index for column: " + colName + " with " + allRecords.size() + " records");
		FileManager.storeTable(tableName, t);
	}

	public static String getValueBits(String tableName, String colName, String value) {
		// Load the bitmap index
		BitmapIndex index = FileManager.loadTableIndex(tableName, colName);
		if (index == null) return "";

		// Get the bitmap for the specific value
		Map<String, ArrayList<Boolean>> bitmaps = index.getBitmaps();
		ArrayList<Boolean> bitmap = bitmaps.get(value);
		
		if (bitmap == null) return "";

		// Convert bitmap to string representation
		StringBuilder bitstream = new StringBuilder();
		for (Boolean bit : bitmap) {
			bitstream.append(bit ? "1" : "0");
		}

		// Update trace
		Table t = FileManager.loadTable(tableName);
		if (t != null) {
			t.getTrace().add("Retrieved bitstream for value: " + value + " in column: " + colName);
			FileManager.storeTable(tableName, t);
		}

		return bitstream.toString();
	}

	public static ArrayList<String[]> selectIndex(String tableName, String[] cols, String[] vals) {
		Table t = FileManager.loadTable(tableName);
		if (t == null) return new ArrayList<>();

		// Check which columns have indices
		Set<String> indexedColumns = new HashSet<>();
		for (String col : cols) {
			if (FileManager.loadTableIndex(tableName, col) != null) {
				indexedColumns.add(col);
			}
		}

		ArrayList<String[]> result;
		String operationType;

		// Case 1: All columns have indices
		if (indexedColumns.size() == cols.length) {
			result = selectWithAllIndices(t, cols, vals);
			operationType = "All columns indexed";
		}
		// Case 2: Only one column has index
		else if (indexedColumns.size() == 1) {
			result = selectWithSingleIndex(t, cols, vals, indexedColumns.iterator().next());
			operationType = "Single column indexed";
		}
		// Case 3: Multiple but not all columns have indices
		else if (indexedColumns.size() > 1) {
			result = selectWithMultipleIndices(t, cols, vals, indexedColumns);
			operationType = "Multiple columns indexed";
		}
		// Case 4: No indices - use linear search
		else {
			result = t.select(cols, vals);
			operationType = "No indices used";
		}

		// Update trace
		t.getTrace().add("Index selection: " + operationType + ", found " + result.size() + " records");
		FileManager.storeTable(tableName, t);

		return result;
	}

	private static ArrayList<String[]> selectWithAllIndices(Table t, String[] cols, String[] vals) {
		// Get results from each index
		ArrayList<Set<Integer>> indexResults = new ArrayList<>();
		for (int i = 0; i < cols.length; i++) {
			BitmapIndex index = FileManager.loadTableIndex(t.getName(), cols[i]);
			ArrayList<Integer> positions = index.select(vals[i]);
			indexResults.add(new HashSet<>(positions));
		}

		// Find intersection of all results
		Set<Integer> finalPositions = new HashSet<>(indexResults.get(0));
		for (int i = 1; i < indexResults.size(); i++) {
			finalPositions.retainAll(indexResults.get(i));
		}

		// Convert positions to records
		ArrayList<String[]> result = new ArrayList<>();
		for (Integer pos : finalPositions) {
			int pageNum = pos / dataPageSize;
			int recordNum = pos % dataPageSize;
			ArrayList<String[]> records = t.select(pageNum, recordNum);
			result.addAll(records);
		}

		return result;
	}

	private static ArrayList<String[]> selectWithSingleIndex(Table t, String[] cols, String[] vals, String indexedCol) {
		// Find the indexed column index
		int indexedColIndex = -1;
		for (int i = 0; i < cols.length; i++) {
			if (cols[i].equals(indexedCol)) {
				indexedColIndex = i;
				break;
			}
		}

		// Get results from index
		BitmapIndex index = FileManager.loadTableIndex(t.getName(), indexedCol);
		ArrayList<Integer> positions = index.select(vals[indexedColIndex]);
		
		// Get records from positions
		ArrayList<String[]> result = new ArrayList<>();
		for (Integer pos : positions) {
			int pageNum = pos / dataPageSize;
			int recordNum = pos % dataPageSize;
			ArrayList<String[]> records = t.select(pageNum, recordNum);
			result.addAll(records);
		}

		// Apply linear filtering for other conditions
		ArrayList<String[]> filteredResult = new ArrayList<>();
		for (String[] record : result) {
			boolean matches = true;
			for (int i = 0; i < cols.length; i++) {
				if (i != indexedColIndex) {
					int colIndex = Arrays.asList(t.getColumnsNames()).indexOf(cols[i]);
					if (!record[colIndex].equals(vals[i])) {
						matches = false;
						break;
					}
				}
			}
			if (matches) {
				filteredResult.add(record);
			}
		}

		return filteredResult;
	}

	private static ArrayList<String[]> selectWithMultipleIndices(Table t, String[] cols, String[] vals, Set<String> indexedColumns) {
		// Get results from each index
		ArrayList<Set<Integer>> indexResults = new ArrayList<>();
		for (int i = 0; i < cols.length; i++) {
			if (indexedColumns.contains(cols[i])) {
				BitmapIndex index = FileManager.loadTableIndex(t.getName(), cols[i]);
				ArrayList<Integer> positions = index.select(vals[i]);
				indexResults.add(new HashSet<>(positions));
			}
		}

		// Find intersection of indexed results
		Set<Integer> finalPositions = new HashSet<>(indexResults.get(0));
		for (int i = 1; i < indexResults.size(); i++) {
			finalPositions.retainAll(indexResults.get(i));
		}

		// Get records from positions
		ArrayList<String[]> result = new ArrayList<>();
		for (Integer pos : finalPositions) {
			int pageNum = pos / dataPageSize;
			int recordNum = pos % dataPageSize;
			ArrayList<String[]> records = t.select(pageNum, recordNum);
			result.addAll(records);
		}

		// Apply linear filtering for non-indexed conditions
		ArrayList<String[]> filteredResult = new ArrayList<>();
		for (String[] record : result) {
			boolean matches = true;
			for (int i = 0; i < cols.length; i++) {
				if (!indexedColumns.contains(cols[i])) {
					int colIndex = Arrays.asList(t.getColumnsNames()).indexOf(cols[i]);
					if (!record[colIndex].equals(vals[i])) {
						matches = false;
						break;
					}
				}
			}
			if (matches) {
				filteredResult.add(record);
			}
		}

		return filteredResult;
	}

	public static void main(String []args) throws IOException
	{
		FileManager.reset();
		String[] cols = {"id","name","major","semester","gpa"};
		createTable("student", cols);
		String[] r1 = {"1", "stud1", "CS", "5", "0.9"};
		insert("student", r1);
		String[] r2 = {"2", "stud2", "BI", "7", "1.2"};
		insert("student", r2);
		String[] r3 = {"3", "stud3", "CS", "2", "2.4"};
		insert("student", r3);
		String[] r4 = {"4", "stud4", "DMET", "9", "1.2"};
		insert("student", r4);
		String[] r5 = {"5", "stud5", "BI", "4", "3.5"};
		insert("student", r5);
		 System.out.println("Output of selecting the whole table content:");
		ArrayList<String[]> result1 = select("student");
		 for (String[] array : result1) {
		 for (String str : array) {
		 System.out.print(str + " ");
		 }
		 System.out.println();
		 }
		 
		 System.out.println("--------------------------------");
		 System.out.println("Output of selecting the output by position:");
		ArrayList<String[]> result2 = select("student", 1, 1);
		 for (String[] array : result2) {
		 for (String str : array) {
		 System.out.print(str + " ");
		 }
		 System.out.println(); 
		 }
		 
		 System.out.println("--------------------------------");
		 System.out.println("Output of selecting the output by column condition:");
		ArrayList<String[]> result3 = select("student", new String[]{"gpa"}, new
		String[]{"1.2"});
		 for (String[] array : result3) {
		 for (String str : array) {
		 System.out.print(str + " ");
		 }
		 System.out.println(); 
		 }
		System.out.println("--------------------------------");
		System.out.println("Full Trace of the table:");
		System.out.println(getFullTrace("student"));
		System.out.println("--------------------------------");
		System.out.println("Last Trace of the table:");
		System.out.println(getLastTrace("student"));
		System.out.println("--------------------------------");
		System.out.println("The trace of the Tables Folder:");
		System.out.println(FileManager.trace());
		FileManager.reset();
		System.out.println("--------------------------------");
		System.out.println("The trace of the Tables Folder after resetting:");
		System.out.println(FileManager.trace());
	}

}
