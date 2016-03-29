<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>

<jsp:include page="header.jsp" >	
	<jsp:param name="selected" value="${object.id}" />
</jsp:include>

<h2>Objeto: <span>${object.id}</span></h2>

<c:forEach items="${object.content.sections}" var="section" varStatus = "status">
	<details ${status.first ? 'open="open"' : ''}>	
		<summary>${section.name}</summary>
	
		<c:forEach items="${section.fields}" var="field"> 
			<p class="fieldAndValue">${field.name}<span class="field"></span><span class="value">${field.value}</span></p>
		</c:forEach>	
	</details>
</c:forEach>

<h2>Objetos similares: De mayor a menor similitud</h2>


<c:set var="relevanceStyleCounter" value="1" scope="page" />

<c:forEach items="${object.similitudes.coincidencesByRelevance}" var="coincidenceGroup">	
		
	<details class="coincidence-${relevanceStyleCounter}">
	
		<summary>Coincidencias de ${coincidenceGroup.attbsNumber} atributos</summary>
		
		<c:forEach items="${coincidenceGroup.coincidences}" var="coincidence">
			<details>
				<summary>Objetos similares: ${coincidence.numberOfObjects}</summary>
				
				<p>Objetos</p>
				
				<c:forEach items="${coincidence.objects}" var="similarObject">
					<c:url value="/object/${similarObject}" var="objectLink" />
					
					<a href="${objectLink}" target="_blank">${similarObject}</a>&nbsp;
				</c:forEach>
				
				<p>Campos coincidentes</p>
				
				<c:forEach items="${coincidence.fields}" var="simlarField">								
					${simlarField}&nbsp;
				</c:forEach>			
			</details>	
		</c:forEach>
	</details>
	
	<c:set var="relevanceStyleCounter" value="${relevanceStyleCounter + 1}" scope="page"/>
</c:forEach>
 	
<jsp:include page="footer.jsp" />