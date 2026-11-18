package swiss.sib.swissprot.servicedescription;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;

/**
 * Splits a plain PURL-style string into a namespace + local name pair and
 * builds an {@link IRI} from them.
 *
 * This used to be implemented three times, independently, with a real
 * behavioural difference between them: {@code GraphDescription.valueOf}
 * split only on the last {@code '/'}, while
 * {@code ServiceDescriptionStatementGenerator.getIRI} (both overloads) also
 * special-cased a trailing {@code '/'} so the local name is the last path
 * segment rather than empty (e.g. {@code ".../foo/bar/"} becomes namespace
 * {@code ".../foo/"} + name {@code "bar"}, not namespace {@code ".../foo/bar/"}
 * + an empty name). This is now the one implementation all three delegate
 * to, using the more defensive (trailing-'/'-aware) behaviour throughout:
 * every input that previously worked under either version still resolves
 * the same way, since real graph/class/predicate IRIs are not bare PURLs
 * ending in '/'.
 */
public final class PurlResolver {

	private PurlResolver() {
	}

	public static IRI resolve(final String rawPurl, ValueFactory vf) {
		if (rawPurl.endsWith("/")) {
			final int position = Math.max(rawPurl.lastIndexOf('/', rawPurl.length() - 2),
					rawPurl.lastIndexOf('#'));
			if (position == -1) {
				throw new IllegalArgumentException("Invalid PURL: " + rawPurl);
			}
			String namespace = rawPurl.substring(0, position + 1);
			String name = rawPurl.substring(position + 1, rawPurl.length() - 1);
			return vf.createIRI(namespace, name);
		} else {
			final int position = Math.max(rawPurl.lastIndexOf('/'), rawPurl.lastIndexOf('#'));
			if (position == -1) {
				throw new IllegalArgumentException("Invalid PURL: " + rawPurl);
			}
			String namespace = rawPurl.substring(0, position + 1);
			String name = rawPurl.substring(position + 1);
			return vf.createIRI(namespace, name);
		}
	}
}
