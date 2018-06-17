package semantics;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.io.FilenameUtils;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import semantics.result.GraphResult;

/**
 * Created by jbarrasa on 21/03/2016.
 * <p>
 * RDF importer based on: 1. DatatypeProperties become node attributes 2.
 * rdf:type relationships are transformed into labels on the subject node 3.
 * rdf:type relationships generate :Class nodes on the object
 */
public class RDFImport {
	private static final boolean DEFAULT_SHORTEN_URLS = true;
	private static final boolean DEFAULT_TYPES_TO_LABELS = true;
	private static final long DEFAULT_COMMIT_SIZE = 25000;
	private static final long DEFAULT_NODE_CACHE_SIZE = 10000;
	public static final String PREFIX_SEPARATOR = "__";

	@Context
	public GraphDatabaseService db;
	@Context
	public Log log;

	public static RDFFormat[] availableParsers = new RDFFormat[] { RDFFormat.RDFXML, RDFFormat.JSONLD, RDFFormat.TURTLE,
			RDFFormat.NTRIPLES, RDFFormat.TRIG };

	@Procedure(mode = Mode.WRITE)
	public Stream<ImportResults> importRDF(@Name("url") String url, 
			@Name("format") String format,
			@Name("props") Map<String, Object> props) {

		final boolean shortenUrls = (props.containsKey("shortenUrls") ? (boolean) props.get("shortenUrls") : DEFAULT_SHORTEN_URLS);
		final boolean typesToLabels = (props.containsKey("typesToLabels") ? (boolean) props.get("typesToLabels") : DEFAULT_TYPES_TO_LABELS);
		final long commitSize = (props.containsKey("commitSize") ? (long) props.get("commitSize") : DEFAULT_COMMIT_SIZE);
		final long nodeCacheSize = (props.containsKey("nodeCacheSize") ? (long) props.get("nodeCacheSize") : DEFAULT_NODE_CACHE_SIZE);
		final String languageFilter = (props.containsKey("languageFilter") ? (String) props.get("languageFilter") : null);

		ImportResults importResults = new ImportResults();
		
		try {
			List<String> listOfUrls = getTTLUrls(url);
			for (String oneUrl : listOfUrls) {

				DirectStatementLoader statementLoader = new DirectStatementLoader(db,
						(commitSize > 0 ? commitSize : 5000), nodeCacheSize, shortenUrls, typesToLabels, languageFilter,
						log);

				ImportResults iResults = importRDFProcessor(oneUrl, format, props, statementLoader);
				importResults.setNamespaces(iResults.namespaces);
				importResults.setTriplesLoaded(iResults.triplesLoaded);
				importResults.setTerminationKO(iResults.extraInfo);
			}
		} catch (MalformedURLException ex) {
			ex.printStackTrace();
		}
		
		return Stream.of(importResults);
		
	}

	private ImportResults importRDFProcessor(String url, String format, Map<String, Object> props, DirectStatementLoader statementLoader) {
		
		ImportResults importResults = new ImportResults();
		
		URLConnection urlConn;
		
		try {
			checkIndexesExist();

			urlConn = new URL(url).openConnection();
			if (props.containsKey("headerParams")) {
				((Map<String, String>) props.get("headerParams")).forEach((k, v) -> urlConn.setRequestProperty(k, v));
			}
			InputStream inputStream = urlConn.getInputStream();
			RDFParser rdfParser = Rio.createParser(getFormat(format));
			rdfParser.setRDFHandler(statementLoader);
			rdfParser.parse(inputStream, url);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException
				| RDFImportPreRequisitesNotMet e) {
			importResults.setTerminationKO(e.getMessage());
			e.printStackTrace();
		} finally {
			importResults.setTriplesLoaded(statementLoader.getIngestedTriples());
			importResults.setNamespaces(statementLoader.getNamespaces());
		}
		
		return importResults;
	}

	/**
	 * Accepts a url of the form: http:// or file://
	 * If url protocol is file:// and is directory, collects all ttl files in subfolders
	 * @param url
	 * @return a list of one or more urls
	 * @throws MalformedURLException
	 */
	private List<String> getTTLUrls(String url) throws MalformedURLException {
		
		List<String> listOfUrls = new ArrayList<String>();

		URL lURL = new URL(url);
		
		//Handle only file protocol else ignore http protocol
		if ("file".equalsIgnoreCase(lURL.getProtocol())) {
			String file = lURL.getFile();
			//Handle file = directory
			if (new File(file).isDirectory()) {
				listOfUrls = loadTTLFiles(file);
			} else {
				listOfUrls.add(url);
			}
		} else {
			listOfUrls.add(url);
		}
		
		return listOfUrls;
	}

	/**
	 * Recursively load all ttl files it finds in subdirectories
	 * @param directoryName
	 * @return list of fullpath of ttl files
	 */
	private static List<String> loadTTLFiles(String directoryName) {

		File directory = new File(directoryName);

		List<String> lFiles = new ArrayList<String>();

		// Get all the ttl files from the sub-directory
		File[] fList = directory.listFiles();
		for (File file : fList) {
			if (file.isFile()) {

				//Skip files which are NOT ttl
				if (!FilenameUtils.getExtension(file.getName()).equalsIgnoreCase("ttl")) {
					continue;
				}
				
				lFiles.add("file:" + file.toPath().toString());
				
			} else if (file.isDirectory()) {
				lFiles.addAll(loadTTLFiles(file.getAbsolutePath()));
			}
		}
		return lFiles;
	}

	@Procedure(mode = Mode.READ)
	public Stream<GraphResult> previewRDF(@Name("url") String url, @Name("format") String format,
			@Name("props") Map<String, Object> props) {

		final boolean shortenUrls = (props.containsKey("shortenUrls") ? (boolean) props.get("shortenUrls")
				: DEFAULT_SHORTEN_URLS);
		final boolean typesToLabels = (props.containsKey("typesToLabels") ? (boolean) props.get("typesToLabels")
				: DEFAULT_TYPES_TO_LABELS);
		final String languageFilter = (props.containsKey("languageFilter") ? (String) props.get("languageFilter")
				: null);

		URLConnection urlConn;
		Map<String, Node> virtualNodes = new HashMap<>();
		List<Relationship> virtualRels = new ArrayList<>();

		StatementPreviewer statementViewer = new StatementPreviewer(db, shortenUrls, typesToLabels, virtualNodes,
				virtualRels, languageFilter, log);
		try {
			urlConn = new URL(url).openConnection();
			if (props.containsKey("headerParams")) {
				((Map<String, String>) props.get("headerParams")).forEach((k, v) -> urlConn.setRequestProperty(k, v));
			}
			InputStream inputStream = urlConn.getInputStream();
			RDFFormat rdfFormat = getFormat(format);
			log.info("Data set to be parsed as " + rdfFormat);
			RDFParser rdfParser = Rio.createParser(rdfFormat);
			rdfParser.setRDFHandler(statementViewer);
			rdfParser.parse(inputStream, "http://neo4j.com/base/");
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException
				| RDFImportPreRequisitesNotMet e) {
			e.printStackTrace();
		}

		GraphResult graphResult = new GraphResult(new ArrayList<>(virtualNodes.values()), virtualRels);
		return Stream.of(graphResult);

	}

	@Procedure(mode = Mode.READ)
	public Stream<GraphResult> previewRDFSnippet(@Name("rdf") String rdfFragment, @Name("format") String format,
			@Name("props") Map<String, Object> props) {

		final boolean shortenUrls = (props.containsKey("shortenUrls") ? (boolean) props.get("shortenUrls")
				: DEFAULT_SHORTEN_URLS);
		final boolean typesToLabels = (props.containsKey("typesToLabels") ? (boolean) props.get("typesToLabels")
				: DEFAULT_TYPES_TO_LABELS);
		final String languageFilter = (props.containsKey("languageFilter") ? (String) props.get("languageFilter")
				: null);

		Map<String, Node> virtualNodes = new HashMap<>();
		List<Relationship> virtualRels = new ArrayList<>();

		StatementPreviewer statementViewer = new StatementPreviewer(db, shortenUrls, typesToLabels, virtualNodes,
				virtualRels, languageFilter, log);
		try {
			InputStream inputStream = new ByteArrayInputStream(rdfFragment.getBytes(Charset.defaultCharset())); // rdfFragment.openStream();
			RDFFormat rdfFormat = getFormat(format);
			log.info("Data set to be parsed as " + rdfFormat);
			RDFParser rdfParser = Rio.createParser(rdfFormat);
			rdfParser.setRDFHandler(statementViewer);
			rdfParser.parse(inputStream, "http://neo4j.com/base/");
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException
				| RDFImportPreRequisitesNotMet e) {
			e.printStackTrace();
		}

		GraphResult graphResult = new GraphResult(new ArrayList<>(virtualNodes.values()), virtualRels);
		return Stream.of(graphResult);

	}

	private void checkIndexesExist() throws RDFImportPreRequisitesNotMet {
		Iterable<IndexDefinition> indexes = db.schema().getIndexes();
		if (missing(indexes.iterator(), "Resource")) {
			throw new RDFImportPreRequisitesNotMet("The required index on :Resource(uri) could not be found");
		}
	}

	private boolean missing(Iterator<IndexDefinition> iterator, String indexLabel) {
		while (iterator.hasNext()) {
			IndexDefinition indexDef = iterator.next();
			if (indexDef.getLabel().name().equals(indexLabel)
					&& indexDef.getPropertyKeys().iterator().next().equals("uri")) {
				return false;
			}
		}
		return true;
	}

	private RDFFormat getFormat(String format) throws RDFImportPreRequisitesNotMet {
		if (format != null) {
			for (RDFFormat parser : availableParsers) {
				if (parser.getName().equals(format))
					return parser;
			}
		}
		throw new RDFImportPreRequisitesNotMet("Unrecognized serialization format: " + format);
	}

	public static class ImportResults {
		public String terminationStatus = "OK";
		public long triplesLoaded = 0;
		public Map<String, String> namespaces = new HashMap<String, String>();
		public String extraInfo = "";

		public void setTriplesLoaded(long triplesLoaded) {
			this.triplesLoaded += triplesLoaded;
		}

		public void setNamespaces(Map<String, String> namespaces) {
			this.namespaces.putAll(namespaces);
		}

		public void setTerminationKO(String message) {
			this.terminationStatus = "KO";
			this.extraInfo = message;
		}

	}

	private class RDFImportPreRequisitesNotMet extends Exception {
		String message;

		public RDFImportPreRequisitesNotMet(String s) {
			message = s;
		}

		@Override
		public String getMessage() {
			return message;
		}
	}
}
