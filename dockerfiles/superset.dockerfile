FROM apache/superset:GHA-16814370361

USER root

COPY ./entrypoints/superset.sh .
RUN chmod u+x ./superset.sh

ENTRYPOINT ["./superset.sh"]

USER 1001