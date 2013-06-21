TreeMap<String, Buffer> sField=new TreeMap<String, Buffer>();
TreeMap<String, TreeMap<String, Buffer>> mField=new TreeMap<String, TreeMap<String, Buffer>>();
TreeMap<String, ArrayList<TreeMap<String, Buffer>>> lField = new TreeMap<String, ArrayList<TreeMap<String, Buffer>>>();
sField.put("cat", new Buffer(tmp.toString().getBytes()));
MulMap arecord= new MulMap(sField,mField,lField);
