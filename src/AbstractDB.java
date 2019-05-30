import exceptions.DBException;
import java.io.FileNotFoundException;
import java.util.*;

/**
 * DavidDB: The finest relational database system written in the common era.
 *
 * @author David
 * @version 9/25/18
 */
public abstract class AbstractDB {
	protected String schema_file;
	protected Map<String, AbstractRelation> relations;

	/**
	 * Creates a new instance of DavidDB.
	 *
	 * @param filename path to the schema file.
	 */
	public AbstractDB(String filename) throws FileNotFoundException {
		this.schema_file = filename;
		this.relations = new HashMap<>();
		this.createRelations();
	}

	/**
	 * Creates (but does not populate) the relations specified in the schema file.
	 *
	 * @throws FileNotFoundException if the schema file does not exist
	 * @throws DBException           if the an unrecognized data type is detected
	 */
	public abstract void createRelations() throws FileNotFoundException;

	/**
	 * Gets a reference to the stored relation with the given name
	 *
	 * @param name the name of the relation (case sensitive)
	 * @return relation with the given name, or null if not exists
	 */
	public abstract AbstractRelation getRelation(String name);

	/**
	 * Generates and returns a string containing all the relations defined
	 * in this database in no particular order.
	 *
	 * @return string containing R1(a1,..) followed by R2(a1,..), etc.
	 */
	@Override
	public abstract String toString();

	/**
	 * (Hwk 2 addition)
	 * Performs intersection between two relations
	 *
	 * @param first  one relation
	 * @param second second relation
	 * @return a relation containing the intersection
	 * @throws DBException if relations are incompatible
	 */
	public abstract Relation intersect(Relation first, Relation second) throws DBException;

	/**
	 * (Hwk 2 addition)
	 * Performs union between this and the given relation
	 *
	 * @param first  one relation
	 * @param second second relation
	 * @return a relation containing the union
	 * @throws DBException if relations are incompatible
	 */
	public abstract Relation union(Relation first, Relation second) throws DBException;

	/**
	 * (Hwk 2 addition)
	 * Performs a set difference between this and the given relation
	 *
	 * @param first  one relation
	 * @param second second relation
	 * @return a relation containing the difference
	 * @throws DBException if relations are incompatible
	 */
	public abstract Relation minus(Relation first, Relation second) throws DBException;

	/**
	 * (Hwk 2 addition)
	 * Performs a cartesian product between two relations
	 *
	 * @param first  one relation
	 * @param second second relation
	 * @return a relation containing the cartesian product, or null if either is null
	 */
	public abstract Relation times(Relation first, Relation second);

	/**
	 * (Hwk3 addition)
	 * Evaluates the specified condition on the current relation
	 *
	 * @param r        the relation to perform the selection
	 * @param cond_str a boolean condition
	 * @return a reference to a relation which stores only the tuples
	 * for which the condition evaluated true
	 * @throws DBException if the given condition is invalid
	 */
	public abstract Relation select(Relation r, String cond_str) throws DBException;

	/**
	 * (Hwk3 addition)
	 * This method accepts a list of Attributes, and retains only the values
	 * pertaining to those attributes, for each tuple.
	 *
	 * @param r               the relation to perform the project
	 * @param projection_list an array of pedantic attribute names (i.e., "R.A") to project
	 * @return a reference to a relation with the projected attributes, or null
	 * if no attributes are given.
	 */
	public abstract Relation project(Relation r, String[] projection_list) throws DBException;

	/**
	 * (Hwk3 addition)
	 * Performs a natural join between two relations.
	 * @param r1	first relation
	 * @param r2	second relation
	 * @return a reference to a relation containing the joined data
	 */
	public abstract Relation naturalJoin(Relation r1, Relation r2) throws DBException;

	/**
	 * (Hwk3 addition)
	 * Renames the given relation.
	 *
	 * @param r       the relation to rename
	 * @param newName a new name
	 * @return the given relation after renaming
	 */
	public abstract Relation renameRelation(Relation r, String newName);

	/**
	 * (Hwk3 addition)
	 * Renames the list of attributes. Takes short names (not pedantic ones).
	 *
	 * @param r    the relation whose attributes we want to rename
	 * @param list a list of new attribute names
	 * @return the given relation after renaming
	 * @throws DBException if list size differs from number of attributes
	 */
	public abstract Relation renameAttributes(Relation r, String[] list) throws DBException;

	/**
	 * (Hwk 5 addition)
	 * Aggregates, possibly over group(s), across the given relation
	 * @param r			relation over which to aggregate
	 * @param agg_fns	a list of aggregation functions (see: Agg enum)
	 * @param attrs		names of the attribute to apply the aggregation function
	 * @return	a relation containing the group(s) and aggregated value
	 * @throws DBException if attribute name or any groups are unknown, or if aggregation function cannot be performed
	 */
	public abstract Relation aggregate(Relation r, Agg[] agg_fns, String[] attrs)
			throws DBException;

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
	public abstract Relation aggregate(Relation r, Agg[] agg_fns, String[] attrs, String[] groups)
			throws DBException;


	/**
	 * (Hwk 6 addition)
	 * Performs a natural join between two relations using the hash-join algorithm.
	 * @param r1	first relation
	 * @param r2	second relation
	 * @return a reference to a relation containing the joined data
	 * @throws DBException
	 * @pre the common attributes in r1 must be unique
	 */
	public abstract Relation hashJoin(Relation r1, Relation r2) throws DBException;
}