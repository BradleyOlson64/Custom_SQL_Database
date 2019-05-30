import exceptions.*;
import perf.Timeable;
import solver.*;
import java.util.*;
import java.lang.*;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * DavidDB: The finest relational database system written in the common era.
 *
 * @author David
 * @version 5/25/18
 */
public class DavidDB extends AbstractDB implements Timeable {
	protected double timeElapsed = 0;
	
	/**
	 * Creates a new instance of DavidDB.
	 * @param filename	path to the schema file.
	 */
	public DavidDB(String filename) throws FileNotFoundException {
		super(filename);
	}

	/**
	 * Creates (but does not populate) the relations specified in the schema file.
	 * @throws FileNotFoundException if the schema file does not exist
	 * @throws DBException if the an unrecognized data type is detected
	 */
	@Override
	public void createRelations() throws FileNotFoundException {
		BufferedReader fin = new BufferedReader(new FileReader(this.schema_file));
		String line;
		try {
			// each line contains: rel(type1 a1, type2 a2,...)
			while ((line = fin.readLine()) != null) {
				if (line.matches("^.+\\((.,?)+\\)$")) {
					// get relation name and instantiate relation
					String rel_name = line.substring(0, line.indexOf("("));

					// instantiate new relation
					Relation newRelation = new Relation(rel_name);

					// build attribute list
					List<Attribute> list = new ArrayList<>();
					String[] attr_token = line.substring(line.indexOf("(")).
							replaceAll("[()]","").split(",");

					// each attr_token is: "ATTRIBUTE_NAME TYPE"
					for (String attr_str : attr_token) {
						String[] pair = attr_str.trim().split("\\s+");
						pair[1] = pair[1].toUpperCase();
						switch (pair[1]) {
							case "TEXT":
								list.add(new Attribute(newRelation, Attribute.Type.TEXT, pair[0]));
								break;
							case "NUMERIC":
								list.add(new Attribute(newRelation, Attribute.Type.NUMERIC, pair[0]));
								break;
							default:
								throw new DBException("Unrecognized data type for attribute " + pair[0] +
										": " + pair[1]);
						}
					}
					// add attributes
					newRelation.setAttributes(list);

					// instantiate the relation
					this.relations.put(rel_name, newRelation);
				}
			}
			fin.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Gets a reference to the stored relation with the given name
	 * @param name	the name of the relation (case sensitive)
	 * @return	relation with the given name, or null if not exists
	 */
	@Override
	public AbstractRelation getRelation(String name) {
		return relations.getOrDefault(name, null);
	}

	/**
	 * Generates and returns a string containing all the relations defined
	 * in this database in no particular order.
	 *
	 * @return string containing R1(a1,..) followed by R2(a1,..), etc.
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Collection<AbstractRelation> relations = this.relations.values();
		for (AbstractRelation r : relations) {
			sb.append(r.schemaToString()).append("\n");
		}
		return sb.toString();
	}

	/**
	 * (Hwk 2 addition)
	 * Performs intersection between two relations
	 * @param first		one relation
	 * @param second	second relation
	 * @return a relation containing the intersection
	 * @throws DBException if relations are incompatible
	 */
	@Override
	public Relation intersect(Relation first, Relation second) throws DBException {
		return this.minus(first, this.minus(first,second));
	}

	/**
	 * (Hwk 2 addition)
	 * Performs union between this and the given relation
	 * @param first		one relation
	 * @param second	second relation
	 * @return a relation containing the union
	 * @throws DBException if relations are incompatible
	 */
	@Override
	public Relation union(Relation first, Relation second) throws DBException {
		if (first == null || second == null) {
			return null;
		}
		if (!isCompatible(first, second)) {
			throw new DBException("Union: incompatible relations " +
					first.schemaToString() + " and " + second.schemaToString());
		}
		double curr = System.currentTimeMillis();
		// generate a new relation containing the union
		Relation new_relation = (Relation) first.clone();
		new_relation.getTuples().addAll(second.getTuples());
		timeElapsed += (System.currentTimeMillis()-curr);
		return new_relation;
	}

	/**
	 * (Hwk 2 addition)
	 * Performs a set difference between this and the given relation
	 * @param first		one relation
	 * @param second	second relation
	 * @return a relation containing the difference
	 * @throws DBException if relations are incompatible
	 */
	@Override
	public Relation minus(Relation first, Relation second) throws DBException {
		if (first == null || second == null) {
			return null;
		}
		if (!isCompatible(first, second)) {
			throw new DBException("Set difference: incompatible relations" +
					first.schemaToString() + " and " + second.schemaToString());
		}
		double curr = System.currentTimeMillis();
		// generate a new relation containing the diff
		Relation new_relation = (Relation) first.clone();
		new_relation.getTuples().removeAll(second.getTuples());
		timeElapsed += (System.currentTimeMillis()-curr);
		return new_relation;
	}

	/**
	 * (Hwk 2 addition)
	 * Performs a cartesian product between two relations
	 * @param first		one relation
	 * @param second	second relation
	 * @return a relation containing the cartesian product, or null if either is null
	 */
	@Override
	public Relation times(Relation first, Relation second) throws DBException {
		if (first == null || second == null) {
			return null;
		}
		double curr = System.currentTimeMillis();
		Relation left = (Relation) first.clone();
		Relation right = (Relation) second.clone();
		Relation new_relation = new Relation();

		List<Attribute> new_attr = left.getAttributes();
		new_attr.addAll(right.getAttributes());
		new_relation.setAttributes(new_attr);

		Set<Tuple> firstTuples = left.getTuples();
		Set<Tuple> secondTuples = right.getTuples();
		for (Tuple tuple : firstTuples) {
			//concatenate current tuple with another tuple from second table
			for (Tuple other_tuple : secondTuples) {
				Tuple new_tuple = tuple.concat(other_tuple);
				new_tuple.setRelation(new_relation);
				new_relation.addTuple(new_tuple);
			}
		}
		timeElapsed += (System.currentTimeMillis()-curr);
		return new_relation;
	}

	/**
	 * (Hwk 2 addition)
	 * Determines whether two relations are compatible for set operations
	 * @param first		one relation
	 * @param second	second relation
	 * @return true if compatible, false otherwise
	 */
	private boolean isCompatible(Relation first, Relation second) {
		List<Attribute> first_list = first.getAttributes();
		List<Attribute> second_list = second.getAttributes();
		if (first_list.size() == second_list.size()) {
			for (int i = 0; i < first_list.size(); i++) {
				if (!first_list.get(i).getType().equals(second_list.get(i).getType())) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	/**
	 * (Hwk3 addition)
	 * Evaluates the specified condition on the current relation
	 * @param r	the relation to perform the selection
	 * @param cond_str	a boolean condition
	 * @return a reference to a relation which stores only the tuples
	 *          for which the condition evaluated true
	 * @throws DBException if the given condition is invalid
	 */
	@Override
	public Relation select(Relation r, String cond_str) throws DBException {
		double curr = System.currentTimeMillis();
		if (cond_str == null || cond_str.equals("")) {
			timeElapsed += (System.currentTimeMillis()-curr);
			return r;
		}
		Relation result = (Relation) r.clone();
		result.getTuples().clear();

		Set<Tuple> set = r.getTuples();
		for (Tuple candidate : set) {
			if (this.evaluate(candidate, cond_str)) {
				result.addTuple(candidate);
			}
		}
		timeElapsed += (System.currentTimeMillis()-curr);
		return result;
	}

	/**
	 * (Hwk3 addition)
	 * Evaluates a boolean expression given a tuple and its relation
	 * @param t reference to a tuple
	 * @param cond_str	a boolean condition
	 * @return true if condition is true for the given tuple; false otherwise.
	 * @throws DBException if the given condition is invalid
	 */
	private boolean evaluate(Tuple t, String cond_str) throws DBException {
		// split up the condition into tokens
		String[] tokens = cond_str.split("\\s+");

		// run through each expression token and replace a variable with a value (if exists)
		for (int i = 0; i < tokens.length; i++) {
			try {
				tokens[i] = t.valueOf(tokens[i]).toString();
			} catch(DBException e) {
				// ignore
			}
		}
		//re-build the conditional
		StringBuilder sb = new StringBuilder();
		for (String tok : tokens) {
			sb.append(tok + " ");
		}
		return Expression.eval(sb.toString());
	}

	/**
	 * (Hwk3 addition)
	 * This method accepts a list of Attributes, and retains only the values
	 * pertaining to those attributes, for each tuple.
	 * @param r	the relation to perform the project
	 * @param projection_list	an array of attribute names (i.e., "A" or "R.A") to project
	 * @return a reference to a relation with the projected attributes, or null
	 * 			if no attributes are given.
	 * @throws DBException if an attribute name doesn't exist or is ambiguous
	 */
	public Relation project(Relation r, String[] projection_list) throws DBException {
		double curr = System.currentTimeMillis();
		// get attributes of r
		List<Attribute> attributes = r.getAttributes();

		// build attribute list of the project, and also check for ambiguity
		List<Attribute> list = new ArrayList<>();
		for (String attr_name : projection_list) {
			list.add(attributes.get(r.lookup(attr_name)));
		}

		//build new relation
		Relation projection = new Relation();
		projection.setAttributes(list);
		for (Tuple t : r.getTuples()) {
			//build up a tuple; examine and preserve only the given attributes
			List<Comparable> new_values = new ArrayList<>();
			for (int i = 0; i < list.size(); i++) {
				new_values.add(t.valueOf(list.get(i).getPedanticName()));
			}
			Tuple new_tuple = new Tuple(new_values, projection);
			projection.addTuple(new_tuple);
		}
		timeElapsed += (System.currentTimeMillis()-curr);
		return projection;
	}

	/**
	 * (Hwk3 addition)
	 * Performs a natural join between two relations.
	 * @param r1	first relation
	 * @param r2	second relation
	 * @return a reference to a relation containing the joined data
	 */
	@Override
	public Relation naturalJoin(Relation r1, Relation r2) throws DBException {
		// determine common attributes
		Set<Attribute> common = new HashSet<>(r1.getAttributes());
		common.retainAll(r2.getAttributes());
		if (common.size() == 0) {	// no common attributes, natural join reduces to product
			return this.times(r1,r2);
		}

		// build expression to enforce equality
		StringBuilder expr = new StringBuilder();
		int count = 0;
		for (Attribute c : common) {
			if (count > 0) {
				expr.append(" && ");
			}
			expr.append(r1.getName() + "." + c.getName() + " = " + r2.getName() + "." + c.getName());
			count++;
		}
		// build projected attributes in order of: r1.a1, ..., r2.a1, ...
		List<Attribute> project_attr = new ArrayList<>(r1.getAttributes());
		for (Attribute a : r2.getAttributes()) {
			if (!project_attr.contains(a)) {
				project_attr.add(a);
			}
		}
		String[] project_array = new String[project_attr.size()];
		for (int i = 0; i < project_attr.size(); i++) {
			project_array[i] = project_attr.get(i).getPedanticName();
		}
		return this.project(this.select(this.times(r1,r2), expr.toString()), project_array);
	}


	/**
	 * (Hwk3 addition)
	 * Renames the given relation.
	 * @param r	the relation to rename
	 * @param newName	a new name
	 * @return the given relation after renaming
	 */
	@Override
	public Relation renameRelation(Relation r, String newName) {
		double curr = System.currentTimeMillis();
		Relation new_relation = (Relation) r.clone();

		// reset the attributes' relation
		List<Attribute> list = new_relation.getAttributes();
		for (Attribute a : list) {
			a.setRelation(new_relation);
		}
		new_relation.setName(newName);
		new_relation.setAttributes(list);	// this to reset the map with new pedantic names
		timeElapsed += (System.currentTimeMillis()-curr);
		return new_relation;
	}

	/**
	 * (Hwk3 addition)
	 * Renames the list of attributes. Takes short names (not pedantic ones).
	 * @param r the relation whose attributes we want to rename
	 * @param list	a list of new attribute names
	 * @return the given relation after renaming
	 * @throws DBException if list size differs from number of attributes
	 */
	@Override
	public Relation renameAttributes(Relation r, String[] list) throws DBException {
		double curr = System.currentTimeMillis();
		Relation new_relation = (Relation) r.clone();

		// reset the attributes' relation
		List<Attribute> new_list = new_relation.getAttributes();

		// ensure same number of attributes are given
		List<Attribute> attribute_list = new_relation.getAttributes();
		if (attribute_list.size() != list.length) {
			throw new DBException("Attribute size mismatch. Required: " +
					attribute_list.size() + " attributes.");
		}

		// rename each attribute, and reset their relation reference
		for (int i = 0; i < list.length; i++) {
			new_list.get(i).setName(list[i]);
			new_list.get(i).setRelation(r);
		}
		new_relation.setAttributes(new_list);
		timeElapsed += (System.currentTimeMillis()-curr);
		return new_relation;
	}
	
	/**
	 * (Hwk 5 addition)
	 * Aggregates across the given relation
	 * @param r			relation over which to aggregate
	 * @param agg_fns	a list of aggregation functions (see: Agg enum)
	 * @param attrs		names of the attribute to apply the aggregation function
	 * @return	a relation containing the group(s) and aggregated value
	 * @throws DBException if attribute name or any groups are unknown, or if aggregation function cannot be performed
	 */
	@SuppressWarnings("rawtypes")
	public Relation aggregate(Relation r, Agg[] agg_fns, String[] attrs) throws DBException{
		// Making sure each queried attribute is present
		for(String attr : attrs) {
			boolean hasMatch = false;
			for(Attribute inRelation : r.attribute_list) {
				if(attr.equals(inRelation.getName())) hasMatch = true;
			}
			if (hasMatch == false) throw new DBException("At least one queried attribute is not in this relation");
		}
		if(agg_fns.length != attrs.length) throw new DBException("Numbers of functions and attributes must match"); 
		if(agg_fns == null || agg_fns.length == 0) throw new DBException("Must have agregation functions");
		for(String attr: attrs) {
			if(r.attribute_map.get(attr).getCount()>1) throw new DBException("Attribute cannot be ambiguous");
		}
		double curr = System.currentTimeMillis();
		// Running the various aggregation helper functions to obtain aggregate values
		List<Comparable> outputs = new ArrayList<Comparable>();
		List<Attribute.Type> outputTypes = new ArrayList<Attribute.Type>();
		for(int i = 0; i<agg_fns.length; i++) {
			if(agg_fns[i] == Agg.MAX && r.attribute_list.get(r.attribute_map.get(attrs[i]).getPos()).getType() == Attribute.Type.NUMERIC) {
				outputs.add(aggMaxNum(r,attrs[i]));
				outputTypes.add(Attribute.Type.NUMERIC);
			}
			else if(agg_fns[i] == Agg.MAX && r.attribute_list.get(r.attribute_map.get(attrs[i]).getPos()).getType() == Attribute.Type.TEXT) {
				outputs.add(aggMaxStr(r,attrs[i]));
				outputTypes.add(Attribute.Type.TEXT);
			}
			else if(agg_fns[i] == Agg.MIN && r.attribute_list.get(r.attribute_map.get(attrs[i]).getPos()).getType() == Attribute.Type.NUMERIC) {
				outputs.add(aggMinNum(r,attrs[i]));
				outputTypes.add(Attribute.Type.NUMERIC);
			}
			else if(agg_fns[i] == Agg.MIN && r.attribute_list.get(r.attribute_map.get(attrs[i]).getPos()).getType() == Attribute.Type.TEXT) {
				outputs.add(aggMinStr(r,attrs[i]));
				outputTypes.add(Attribute.Type.TEXT);
			}
			else if(agg_fns[i] == Agg.AVG) {
				outputs.add(aggAvg(r,attrs[i],false));
				outputTypes.add(Attribute.Type.NUMERIC);
			}
			else if(agg_fns[i] == Agg.COUNT) {
				outputs.add(aggCount(r,attrs[i],false));
				outputTypes.add(Attribute.Type.NUMERIC);
			}
			else if(agg_fns[i] == Agg.SUM) {
				outputs.add(aggSum(r,attrs[i],false));
				outputTypes.add(Attribute.Type.NUMERIC);
			}
			else if(agg_fns[i] == Agg.AVG_DISTINCT) {
				outputs.add(aggAvg(r,attrs[i],true));
				outputTypes.add(Attribute.Type.NUMERIC);
			}
			else if(agg_fns[i] == Agg.COUNT_DISTINCT) {
				outputs.add(aggCount(r,attrs[i],true));
				outputTypes.add(Attribute.Type.NUMERIC);
			}
			else if(agg_fns[i] == Agg.SUM_DISTINCT) {
				outputs.add(aggSum(r,attrs[i],true));
				outputTypes.add(Attribute.Type.NUMERIC);
			}
		}
		// Constructing new relation
		Relation outputR = new Relation();
		List<Attribute> outAttribs = new ArrayList<Attribute>();
		// Constructing and adding attributes
		for(int i = 0; i<agg_fns.length;i++) {
			Attribute addAttrib = new Attribute(outputR,outputTypes.get(i), agg_fns[i].name() + "(" + attrs[i] + ")");
			outAttribs.add(addAttrib);
		}
		outputR.setAttributes(outAttribs);
		// Constructing and adding tuple
		Tuple addTuple = new Tuple(outputs,outputR);
		outputR.addTuple(addTuple);
		// returning
		timeElapsed += (System.currentTimeMillis()-curr);
		return outputR;
	}

	/**
	 * (Hwk 5 addition)
	 * Aggregates, possibly over group(s), across the given relation
	 * @param r			relation over which to aggregate
	 * @param agg_fns	a list of aggregation functions (see: Agg enum)
	 * @param attrs		names of the attribute to apply the aggregation function
	 * @param groups	a list of groups, or null if no groups
	 * @return	a relation containing the group(s) and aggregated value
	 * @throws DBException if attribute name or any groups are unknown, or if aggregation function cannot be performed
	 */
	public Relation aggregate(Relation r, Agg[] agg_fns, String[] attrs, String[] groups) throws DBException{
		// Making sure each queried attribute is present
		for(String attr : attrs) {
			boolean hasMatch = false;
			for(Attribute inRelation : r.attribute_list) {
				if(attr.equals(inRelation.getName())) hasMatch = true;
			}
			if (hasMatch == false) throw new DBException("At least one queried attribute is not in this relation");
		}
		if(agg_fns == null || agg_fns.length == 0) throw new DBException("Must have agregation functions");
		if(agg_fns.length != attrs.length) throw new DBException("Numbers of functions and attributes must match"); 
		for(String attr: attrs) {
			if(r.attribute_map.get(attr).getCount()>1) throw new DBException("Attribute cannot be ambiguous");
		}
		double curr = System.currentTimeMillis();
		// Performing normal aggregation if there is no grouping
		if(groups == null){
			return aggregate(r,agg_fns,attrs);
		}
		// finding permutations of the grouping
		List<ArrayList<Comparable>> permutations = new ArrayList<ArrayList<Comparable>>();
		for(Tuple tuple:r.tuples) {
			ArrayList<Comparable> permutation = new ArrayList<Comparable>();
			for(String attrib:groups) {
				permutation.add(tuple.valueOf(attrib));
			}
			if(! permutations.contains(permutation)) permutations.add(permutation);
		}
		// Building strings needed to call select
		List<ArrayList<String>> selectStrings = new ArrayList<ArrayList<String>>();
		for(ArrayList<Comparable> permutation: permutations) {
			ArrayList<String> singlePermutationStrings = new ArrayList<String>();
			for(int i=0;i<permutation.size();i++) {
				String addString = groups[i] + " = " + permutation.get(i);
				singlePermutationStrings.add(addString);
			}
			selectStrings.add(singlePermutationStrings);
		}
		// Selecting using given strings
		List<Relation> selected = new ArrayList<Relation>();
		for(ArrayList<String> selectStringsPartial:selectStrings) {
			Relation addSelected = multiSelect(r,selectStringsPartial);
			selected.add(addSelected);
		}
		for(Relation relation:selected) {
		}
		ArrayList<Relation> products = new ArrayList<Relation>();
		for(int i=0;i<agg_fns.length;i++) {
			// Aggregating on selected
			ArrayList<Relation> partials = new ArrayList<Relation>();
			for(Relation partialSelected:selected) {
				// Getting aggregated relation
				Agg[] aggs = new Agg[1];
				String[] attrss = new String[1];
				aggs[0]=agg_fns[i];
				attrss[0]= attrs[i];
				Relation partialAggregated = aggregate(partialSelected,aggs,attrss);
				// Getting relation with a single tuple containing the permutation of grouped atts
				Relation groupingAttribs = project(partialSelected,groups);
				// combining the previous two and adding to list
				partials.add(naturalJoin(groupingAttribs,partialAggregated));
			}
			// Unioning aggregated partials
			Relation nearProduct;
			if(partials.size() == 1) nearProduct = partials.get(0);
			else nearProduct = multiUnion(partials);
			products.add(nearProduct);
		}
		products = multiRename(products);
		// Joining the partial products
		Relation nearProduct;
		if(products.size() == 1) nearProduct = products.get(0);
		else nearProduct = multiJoin(products);
		// Starting grouping case by projecting
		/*List<String> projectList = new ArrayList<String>();
		for (String group:groups) projectList.add(group);
		for (int i = nearProduct.attribute_list.size()-1;i>nearProduct.attribute_list.size()-(1+products.size());i--) {
			 projectList.add(nearProduct.attribute_list.get(i).getName());
		}
		String[] combined = projectList.toArray(new String[projectList.size()]);
		Relation finishedProduct = project(nearProduct,combined);*/
		// Return
		timeElapsed += (System.currentTimeMillis()-curr);
		return nearProduct;
	}
	
	/**
	 * (Hwk 6 addition)
	 * Performs a natural join between two relations using the hash-join algorithm.
	 * @param r1	first relation
	 * @param r2	second relation
	 * @return a reference to a relation containing the joined data
	 * @throws DBException
	 * @pre the common attributes in r1 must be unique
	 */
	public Relation hashJoin(Relation r1, Relation r2) throws DBException{
		double curr = System.currentTimeMillis();
		// determine common attributes
		Set<Attribute> common = new HashSet<>(r1.getAttributes());
		common.retainAll(r2.getAttributes());
		if (common.size() == 0) {	// no common attributes, natural join reduces to product
			timeElapsed += (System.currentTimeMillis()-curr);
			return this.times(r1,r2);
		}
		// Composing map
		HashMap<ArrayList<Comparable>,Tuple> r1Map = new HashMap<>();
		for(Tuple tup: r1.tuples) {
			ArrayList<Comparable> commonVals = new ArrayList<Comparable>();
			for(Attribute attrib: common) {
				commonVals.add(tup.valueOf(attrib.getName()));
			}
			if(r1Map.containsKey(commonVals)) {
				throw new DBException("Common attributes of r1 must have unique values!");
			}
			r1Map.put(commonVals, tup);
		}
		// Joining up with r2
		Relation output = new Relation();
		
		// build projected attributes in order of: r1.a1, ..., r2.a1, ...
		List<Attribute> project_attr = new ArrayList<>(r1.getAttributes());
		for (Attribute a : r2.getAttributes()) {
			if (!project_attr.contains(a)) {
				project_attr.add(a);
			}
		}
		output.setAttributes(project_attr);
		
		// Building tuples
		for(Tuple tup2: r2.tuples) {
			// Finding whether the common values line up with any in the map
			ArrayList<Comparable> commonVals = new ArrayList<Comparable>();
			for(Attribute attrib: common) {
				commonVals.add(tup2.valueOf(attrib.getName()));
			}
			if(r1Map.containsKey(commonVals)) {
				Tuple tup1 = r1Map.get(commonVals);
				// Developing list of values in order to be inserted in new tuple
				ArrayList<Comparable> insertVals = new ArrayList<>();
				for(Comparable val: tup1.data) {
					insertVals.add(val);
				}
				for(int i=0;i<tup2.data.size();i++) {
					if(!common.contains(r2.attribute_list.get(i))) {
						insertVals.add(tup2.data.get(i));
					}
				}
				// Creating tuple
				Tuple addition = new Tuple(insertVals,output);
				output.addTuple(addition);
			}
		}
		
		
		timeElapsed += (System.currentTimeMillis()-curr);
		return output;
	}
	
	/**
	 * @return the elapsed time (in milliseconds) since last reset.
	 */
	public double getElapsedTime() {
		return timeElapsed;
	}

	/**
	 * Resets the elapsed time to zero.
	 */
	public void resetElapsedTime() {
		timeElapsed= 0.0;
	}
	
//----------------------------------------------------------------------------------------------
	private double aggMaxNum(Relation r, String attr) {
		//Fill a list with the values to be aggregated
		List<Double> values =new ArrayList<Double>();
		for(Tuple tuple :r.tuples) {
			values.add((double) tuple.valueOf(attr));
		}
		//Aggregating
		double maximum = values.get(0);
		for(int i=0;i<values.size();i++) {
			if(values.get(i)> maximum) maximum = values.get(i);
		}
		return maximum;
	}
	
	private double aggMinNum(Relation r, String attr) {
		//Fill a list with the values to be aggregated
		List<Double> values =new ArrayList<Double>();
		for(Tuple tuple :r.tuples) {
			values.add((double) tuple.valueOf(attr));
		}
		//Aggregating
		double minimum = values.get(0);
		for(int i=0;i<values.size();i++) {
			if(values.get(i)< minimum) minimum = values.get(i);
		}
		return minimum;
	}
	
	private String aggMaxStr(Relation r, String attr) {
		//Fill a list with the values to be aggregated
		List<String> values =new ArrayList<String>();
		for(Tuple tuple :r.tuples) {
			values.add((String) tuple.valueOf(attr));
		}
		// Aggregating
		String maximum = values.get(0);
		for(int i=0;i<values.size();i++) {
			if(values.get(i).compareTo(maximum)> 0) maximum = values.get(i);
		}
		return maximum;
	}
	
	private String aggMinStr(Relation r, String attr) {
		// Fill a list with the values to be aggregated
		List<String> values =new ArrayList<String>();
		for(Tuple tuple :r.tuples) {
			values.add((String) tuple.valueOf(attr));
		}
		// Aggregating
		String minimum = values.get(0);
		for(int i=0;i<values.size();i++) {
			if(values.get(i).compareTo(minimum)< 0) minimum = values.get(i);
		}
		return minimum;
	}
	private double aggAvg(Relation r, String attr, boolean distinct) throws DBException{
		for(Attribute attribute : r.attribute_list) if(attribute.getName() == attr) if(attribute.getType() == Attribute.Type.TEXT) throw new DBException("This function can't be called on strings");
		// Fill a list with the values to be aggregated
		List<Double> values =new ArrayList<Double>();
		if(distinct == false) {
			for(Tuple tuple :r.tuples) {
				values.add((double) tuple.valueOf(attr));
			}
		}
		else {
			for(Tuple tuple :r.tuples) {
				if(!values.contains((double) tuple.valueOf(attr))) values.add((double) tuple.valueOf(attr));
			}
		}
		// Aggregating
		double sum = 0.0;
		for(double value : values) {
			sum += value;
		}
		return sum/values.size();
	}
	private double aggCount(Relation r, String attr, boolean distinct) throws DBException{
		// Fill a list with the values to be aggregated
		List<Comparable> values =new ArrayList<Comparable>();
		if(distinct == false) {
			for(Tuple tuple :r.tuples) {
				values.add((Comparable) tuple.valueOf(attr));
			}
		}
		else {
			for(Tuple tuple :r.tuples) {
				if(!values.contains((Comparable) tuple.valueOf(attr))) values.add((Comparable) tuple.valueOf(attr));
			}
		}
		// Aggregating
		return (double) values.size();
	}
	
	private double aggSum(Relation r, String attr, boolean distinct) throws DBException{
		for(Attribute attribute : r.attribute_list) if(attribute.getName() == attr) if(attribute.getType() == Attribute.Type.TEXT) throw new DBException("This function can't be called on strings");
		// Fill a list with the values to be aggregated
		List<Double> values =new ArrayList<Double>();
		if(distinct == false) {
			for(Tuple tuple :r.tuples) {
				values.add((double) tuple.valueOf(attr));
			}
		}
		else {
			for(Tuple tuple :r.tuples) {
				if(!values.contains((double) tuple.valueOf(attr))) values.add((double) tuple.valueOf(attr));
			}
		}
		// Aggregating
		double sum = 0.0;
		for(double value : values) {
			sum += value;
		}
		return sum;
	}
	
	private Relation multiSelect(Relation r, ArrayList<String> selectStrings) {
		if(selectStrings.size()==1) return select(r, selectStrings.remove(selectStrings.size()-1)); //shrinks selectStrings
		Relation partialSelect = select(r, selectStrings.remove(selectStrings.size()-1)); //shrinks selectStrings
		return multiSelect(partialSelect,selectStrings);
	}

	private Relation multiUnion(ArrayList<Relation> relations) {
		if(relations.size() == 2) return union(relations.get(0),relations.get(1));
		Relation partialUnion = union(relations.remove(relations.size()-1),relations.remove(relations.size()-1));
		relations.add(partialUnion);
		return multiUnion(relations);
	}

	private Relation multiJoin(ArrayList<Relation> relations) {
		if(relations.size() == 2) return naturalJoin(relations.get(0),relations.get(1));
		Relation partialJoin = naturalJoin(relations.remove(relations.size()-1),relations.remove(relations.size()-1));
		Integer newName = relations.size();
		Relation newPartialJoin = renameRelation(partialJoin,newName.toString());
		relations.add(newPartialJoin);
		return multiJoin(relations);
	}
	
	private ArrayList<Relation> multiRename(ArrayList<Relation> relations) {
		ArrayList<Relation> renamed = new ArrayList<Relation>();
		for(Integer i=0;i<relations.size();i++) {
			renamed.add(renameRelation(relations.get(i),i.toString()));
		}
		return renamed;
	}
	
	
}