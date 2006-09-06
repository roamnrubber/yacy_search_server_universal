// search.java
// -----------------------
// part of the AnomicHTTPD caching proxy
// (C) by Michael Peter Christen; mc@anomic.de
// first published on http://www.anomic.de
// Frankfurt, Germany, 2004
//
// $LastChangedDate$
// $LastChangedRevision$
// $LastChangedBy$
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
// Using this software in any meaning (reading, learning, copying, compiling,
// running) means that you agree that the Author(s) is (are) not responsible
// for cost, loss of data or any harm that may be caused directly or indirectly
// by usage of this softare or this documentation. The usage of this software
// is on your own risk. The installation and usage (starting/running) of this
// software may allow other people or application to access your computer and
// any attached devices and is highly dependent on the configuration of the
// software which must be done by the user of the software; the author(s) is
// (are) also not responsible for proper configuration and usage of the
// software, even if provoked by documentation provided together with
// the software.
//
// Any changes to this file according to the GPL as documented in the file
// gpl.txt aside this file in the shipment you received can be done to the
// lines that follows this copyright notice here, but changes must not be
// done inside the copyright notive above. A re-distribution must contain
// the intact and unchanged copyright notice.
// Contributions and changes to the program code must be marked as such.


// You must compile this file with
// javac -classpath .:../../Classes search.java
// if the shell's current path is htroot/yacy

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import de.anomic.http.httpHeader;
import de.anomic.index.indexContainer;
import de.anomic.index.indexEntryAttribute;
import de.anomic.plasma.plasmaCrawlLURL;
import de.anomic.plasma.plasmaSearchEvent;
import de.anomic.plasma.plasmaSearchRankingProfile;
import de.anomic.plasma.plasmaSearchResult;
import de.anomic.plasma.plasmaSearchTimingProfile;
import de.anomic.plasma.plasmaSnippetCache;
import de.anomic.plasma.plasmaSwitchboard;
import de.anomic.plasma.plasmaSearchQuery;
import de.anomic.server.serverCore;
import de.anomic.server.serverObjects;
import de.anomic.server.serverSwitch;
import de.anomic.yacy.yacyCore;
import de.anomic.yacy.yacySeed;

public final class search {

    public static serverObjects respond(httpHeader header, serverObjects post, serverSwitch ss) {
        if (post == null || ss == null) { return null; }

        // return variable that accumulates replacements
        final plasmaSwitchboard sb = (plasmaSwitchboard) ss;
        
        //System.out.println("yacy: search received request = " + post.toString());

        final String  oseed  = post.get("myseed", ""); // complete seed of the requesting peer
//      final String  youare = post.get("youare", ""); // seed hash of the target peer, used for testing network stability
        final String  key    = post.get("key", "");    // transmission key for response
        final String  query  = post.get("query", "");  // a string of word hashes
//      final String  fwdep  = post.get("fwdep", "");  // forward depth. if "0" then peer may NOT ask another peer for more results
//      final String  fwden  = post.get("fwden", "");  // forward deny, a list of seed hashes. They may NOT be target of forward hopping
        final long    duetime= post.getLong("duetime", 3000);
        final int     count  = post.getInt("count", 10); // maximum number of wanted results
        final int     maxdist= post.getInt("maxdist", Integer.MAX_VALUE);
        final String  prefer = post.get("prefer", "");
        final String  filter = post.get("filter", ".*");
//      final boolean global = ((String) post.get("resource", "global")).equals("global"); // if true, then result may consist of answers from other peers
//      Date remoteTime = yacyCore.parseUniversalDate((String) post.get(yacySeed.MYTIME));        // read remote time

        
        // tell all threads to do nothing for a specific time
        sb.intermissionAllThreads(2 * duetime);

        // store accessing peer
        if (yacyCore.seedDB == null) {
            yacyCore.log.logSevere("yacy.search: seed cache not initialized");
        } else {
            yacyCore.peerActions.peerArrival(yacySeed.genRemoteSeed(oseed, key, true), true);
        }

        // prepare search
        final HashSet keyhashes = new HashSet(query.length() / indexEntryAttribute.wordHashLength);
        for (int i = 0; i < (query.length() / indexEntryAttribute.wordHashLength); i++) {
            keyhashes.add(query.substring(i * indexEntryAttribute.wordHashLength, (i + 1) * indexEntryAttribute.wordHashLength));
        }
        final long timestamp = System.currentTimeMillis();
        
        plasmaSearchQuery squery = new plasmaSearchQuery(keyhashes, maxdist, prefer, count, duetime, filter);
        squery.domType = plasmaSearchQuery.SEARCHDOM_LOCAL;

        serverObjects prop = new serverObjects();

        yacyCore.log.logInfo("INIT HASH SEARCH: " + squery.queryHashes + " - " + squery.wantedResults + " links");
        long timestamp1 = System.currentTimeMillis();
        plasmaSearchRankingProfile rankingProfile = new plasmaSearchRankingProfile(new String[]{plasmaSearchRankingProfile.ORDER_YBR, plasmaSearchRankingProfile.ORDER_DATE, plasmaSearchRankingProfile.ORDER_QUALITY});
        plasmaSearchTimingProfile localTiming  = new plasmaSearchTimingProfile(squery.maximumTime, squery.wantedResults);
        plasmaSearchTimingProfile remoteTiming = null;
        plasmaSearchEvent theSearch = new plasmaSearchEvent(squery, rankingProfile, localTiming, remoteTiming, yacyCore.log, sb.wordIndex, sb.urlPool.loadedURL, sb.snippetCache);
        Set containers = theSearch.localSearchContainers();
        indexContainer localResults = theSearch.localSearchJoin(containers);
        int joincount = localResults.size();
        plasmaSearchResult acc = theSearch.order(localResults);

        // set statistic details of search result
        prop.put("joincount", Integer.toString(joincount));
        if (containers == null) {
            prop.put("indexcount", "");
        } else {
            Iterator ci = containers.iterator();
            StringBuffer indexcount = new StringBuffer();
            while (ci.hasNext()) {
                indexContainer container = (indexContainer) ci.next();
                indexcount.append("indexcount.").append(container.getWordHash()).append('=').append(Integer.toString(container.size())).append(serverCore.crlfString);
            }
            prop.put("indexcount", new String(indexcount));
        }
        
        
        if ((joincount == 0) || (acc == null)) {
            prop.put("links", "");
            prop.put("linkcount", "0");
            prop.put("references", "");
        } else {
            
            // result is a List of urlEntry elements
            int i = 0;
            StringBuffer links = new StringBuffer();
            String resource = "";
            //plasmaIndexEntry pie;
            plasmaCrawlLURL.Entry urlentry;
            plasmaSnippetCache.result snippet;
            while ((acc.hasMoreElements()) && (i < squery.wantedResults)) {
                urlentry = acc.nextElement();
                snippet = sb.snippetCache.retrieve(urlentry.url(), squery.queryHashes, false, 260);
                if (snippet.getSource() == plasmaSnippetCache.ERROR_NO_MATCH) {
                    // suppress line: there is no match in that resource
                } else {
                    if (snippet.exists()) {
                        resource = urlentry.toString(snippet.getLineRaw());
                    } else {
                        resource = urlentry.toString();
                    }
                    if (resource != null) {
                        links.append("resource").append(i).append('=').append(resource).append(serverCore.crlfString);
                        i++;
                    }
                }
            }
            prop.put("links", new String(links));
            prop.put("linkcount", Integer.toString(i));

            // prepare reference hints
            Object[] ws = acc.getReferences(16);
            StringBuffer refstr = new StringBuffer();
            for (int j = 0; j < ws.length; j++)
                refstr.append(",").append((String) ws[j]);
            prop.put("references", (refstr.length() > 0) ? refstr.substring(1) : refstr.toString());
        }
        
        // add information about forward peers
        prop.put("fwhop", ""); // hops (depth) of forwards that had been performed to construct this result
        prop.put("fwsrc", ""); // peers that helped to construct this result
        prop.put("fwrec", ""); // peers that would have helped to construct this result (recommendations)
        
        // log
        yacyCore.log.logInfo("EXIT HASH SEARCH: " + squery.queryHashes + " - " + joincount + " links found, " + prop.get("linkcount", "?") + " links selected, " + ((System.currentTimeMillis() - timestamp1) / 1000) + " seconds");
 
        prop.put("searchtime", Long.toString(System.currentTimeMillis() - timestamp));

        final int links = Integer.parseInt(prop.get("linkcount","0"));
        yacyCore.seedDB.mySeed.incSI(links);
        yacyCore.seedDB.mySeed.incSU(links);
        return prop;
    }

}