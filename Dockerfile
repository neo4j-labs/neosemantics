FROM neo4j:latest
COPY ./target/neosemantics-*.jar plugins/neosemantics.jar
RUN /bin/bash -c 'echo dbms.unmanaged_extension_classes=semantics.extension=/rdf >> conf/neo4j.conf'
VOLUME /var/neo4j/data:/data
VOLUME /var/neo4j/logs:/logs
