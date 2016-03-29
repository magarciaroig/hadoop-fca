<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<!doctype html>

<html lang="es">
<head>
  <meta charset="utf-8">

  <title>Mapa interactivo automático de conceptos: Combinación de Big Data y FCA distribuido</title>
  <meta name="description" content="Mapa interactivo automático de conceptos: Combinación de Big Data y FCA distribuido">
  <meta name="author" content="Miguel Angel García Roig (mgarciaroig@uoc.edu)">

  <link rel="stylesheet" href="css/styles.css?v=1.0">
  
  <link rel="icon" href="http://www.uoc.edu/favicon.ico" type="image/x-icon">
  <link rel="shortcut icon" href="http://www.uoc.edu/favicon.ico" type="image/x-icon">
  
  <style>
	body {font-family: Verdana, sans-serif; font-size:0.8em; color:olive; background-color: #FAF0E6;}
	header,nav, section,article,footer {margin:5px; padding:8px;}
			
	h1 {color: orange; font-size: 1.7em;}
	h2 {color: orange;}
	h2 span {color: red;}
	
	a:hover {background-color: orange;}	
	
	nav{    
  		width: 400px;
  		float: left;
  		display: inline;
  		margin: 0;
  		padding: 0;
  		margin-right: 10px;
  		border: 1px dashed;
  		font-family: monospace;
  		font-size: 1.5em;
  		line-height: 160%;
	}
				
	a {color: olive};		
	a:visited {color: olive;}
	a:hover {color: olive;}
	a:active {color: olive;}
	a.selected {font-weight: bold;}		
	
	nav ul {list-style: none;}
	nav ul li.title {font-weight: bold; margin-bottom: 10px;}
	
	section#main{
  		width: 800px;
  		float: left;
  		margin: 0;
  		padding: 0;
  		padding-left: 20px;
  		display: inline;     
	}
			
	details {margin-top: 10px;}	
	details details {padding-left: 15px;}	
	details details summary {color: olive !important; font-weight: normal !important;}
	details summary {color: olive;}
	
	details.coincidence-1 summary {color: red; font-weight: bold;}
	details.coincidence-2 summary {color: red; font-weight: bold;}
	details.coincidence-3 summary {color: red; font-weight: bold;}
	details.coincidence-4 summary {color: red; font-weight: bold;}
	
	details.coincidence-5 summary {font-weight: bold;}
	details.coincidence-6 summary {font-weight: bold;}
	details.coincidence-7 summary {font-weight: bold;}
	details.coincidence-8 summary {font-weight: bold;}		
	
	p.fieldAndValue:HOVER {background-color: yellow;}
	
	p.fieldAndValue span.field {font-weight: bold; width: 450px; float: left;}	
	p.fieldAndValue span.value {width: 500px; color: red; float: right;}
	
	footer {
		clear: both;
		margin-top: 8px;
		padding-left: 30px;
		font-size: 0.9em;
		font-style: italic;        
	}
	
	footer quote {color: orange; font-weight: bold}
</style>
  
</head>

<body>

	<header>
  		<h1>Mapa interactivo automático de conceptos: Combinación de Big Data y FCA distribuido</h1>
	</header>	
	
	<nav role="main">
		
		<ul>
			<li class="title">Listado de objetos</li>
		
			<c:forEach items="${allObjects}" var="object">
			
				<c:url value="/object/${object.hadoopId}" var="objectLink" />
				
				<c:choose>
    				<c:when test="${param.selected == object.hadoopId}">
        				<c:set var="objectSelectedClass" scope="page" value="selected"/>
    				</c:when>
    				<c:otherwise>
        				<c:set var="objectSelectedClass" scope="page" value="" />
    				</c:otherwise>
				</c:choose>												
		
				<li><a href="${objectLink}" class="${objectSelectedClass}"}>${object.hadoopId}</a></li>		
			</c:forEach>
		</ul>
	</nav>
	
	<section id="main">