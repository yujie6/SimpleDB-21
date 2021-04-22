package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

    public static class CatalogEntry implements Serializable {
        private static final long serialVersionUID = 1L;
        public DbFile file;
        public String name;
        public String pkeyField;
        public CatalogEntry(DbFile dbFile, String n, String p) {
            reset(dbFile, n, p);
        }

        public void reset(DbFile dbFile, String n, String p) {
            this.file = dbFile;
            this.name = n;
            this.pkeyField = p;
        }
    }

    private ArrayList<CatalogEntry> catalogContents;

    public Catalog() {
        catalogContents = new ArrayList<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        for (CatalogEntry entry : catalogContents) {
            if (entry.name.equals(name) || file.getId() == entry.file.getId()) {
                entry.reset(file, name, pkeyField);
            }
        }
        catalogContents.add(new CatalogEntry(file, name, pkeyField));
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        for (CatalogEntry entry : catalogContents) {
            if (entry.name.equals(name)) {
                return entry.file.getId();
            }
        }
        throw new NoSuchElementException();
    }

    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        for (CatalogEntry entry : catalogContents) {
            if (entry.file.getId() == tableid) {
                return entry.file.getTupleDesc();
            }
        }
        throw new NoSuchElementException();
    }

    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        for (CatalogEntry entry : catalogContents) {
            if (entry.file.getId() == tableid) {
                return entry.file;
            }
        }
        throw new NoSuchElementException();
    }

    public String getPrimaryKey(int tableid) {
        for (CatalogEntry entry : catalogContents) {
            if (entry.file.getId() == tableid) {
                return entry.pkeyField;
            }
        }
        throw new NoSuchElementException();
    }

    public Iterator<Integer> tableIdIterator() {
        ArrayList<Integer> tableIds = new ArrayList<>();
        for (CatalogEntry entry : catalogContents) {
            tableIds.add(entry.file.getId());
        }
        return tableIds.iterator();
    }

    public String getTableName(int id) {
        for (CatalogEntry entry : catalogContents) {
            if (entry.file.getId() == id) {
                return entry.name;
            }
        }
        throw new NoSuchElementException();
    }

    public void clear() {
        catalogContents.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

