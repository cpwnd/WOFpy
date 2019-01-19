# -*- coding: utf-8 -*-
"""ORM Definition for WOF to work with ODM2 Model."""
from __future__ import (absolute_import, division, print_function)

from datetime import datetime, timedelta

from wof import models as wof_base


class Variable(wof_base.BaseVariable):
    """WOF Variable Model."""
    def __init__(self, v=None, VarSampleMedium=None,
                 v_unit=None, v_tunit=None, v_timeinterval=None,
                 aggregationstatisticCV=None, actiontypeCV=None):
        """Initialize WOF Variable Object.

        :param v: ODM2 Variable Object
        :param VarSampleMedium: ODM2 Result SampledMediumCV
        :param v_unit: ODM2 Unit Object
        :param v_tunit: ODM2 TimeAggregationIntervalUnits Object
        :param v_timeinterval: ODM2 TimeSeriesResultValues TimeAggregationInterval
        :param aggregationstatisticCV: ODM2 Result AggregationStatisticCV
        :param actiontypeCV: ODM2 Action ActionTypeCV
        """
        self.VariableID = v.VariableID
        self.VariableCode = v.VariableCode
        self.VariableName = v.VariableNameCV
        self.VariableDescription = v.VariableDefinition
        self.NoDataValue = v.NoDataValue
        self.Speciation = v.SpeciationCV
        self.VariableUnitsID = v_unit.UnitsID
        self.GeneralCategory = v.VariableTypeCV

        self.SampleMedium = VarSampleMedium
        self.DataType = aggregationstatisticCV
        self.ValueType = actiontypeCV

        self.GeneralCategoryValidate = False
        self.ValueTypeValidate = False

        if v_unit is not None:
            self.VariableUnits = Unit(v_unit)
        else:
            self.VariableUnits = None
        if v_tunit is not None:
            self.TimeUnits = Unit(v_tunit)
            self.TimeSupport = v_timeinterval
        else:
            self.TimeUnits = None

    # SampleMedium = wof_base.SampleMediumTypes.NOT_RELEVANT
    # IsRegular = Column(Boolean)
    # TimeSupport = Column(Float)
    # TimeUnitsID = Column(Integer, ForeignKey('Units.UnitsID'))

    # VariableUnits = relationship("Units",
    #                    primaryjoin='Variable.VariableUnitsID==Units.UnitsID')

    # TimeUnits = relationship("Units",
    #                    primaryjoin='Variable.TimeUnitsID==Units.UnitsID')


class Site(wof_base.BaseSite):
    """WOF Site Model."""
    def __init__(self, s=None, aff=None):
        """Initialize WOF Site Object.

        :param s: ODM2 SamplingFeature Object
        """
        self.SiteID = s.SamplingFeatureID
        self.Latitude = s.Latitude
        self.Longitude = s.Longitude
        self.LatLongDatumID = s.SpatialReferenceID
        self.SiteCode = s.SamplingFeatureCode
        self.SiteName = s.SamplingFeatureName
        self.Elevation_m = s.Elevation_m
        self.Comments = s.SamplingFeatureDescription
        sr = wof_base.BaseSpatialReference()
        sr.SpatialReferenceId = s.SpatialReferenceObj.SpatialReferenceID
        sr.SRSID = s.SpatialReferenceObj.SRSCode
        sr.SRSName = s.SpatialReferenceObj.SRSName
        sr.Notes = s.SpatialReferenceObj.SRSDescription
        self.LatLongDatum = sr
        if aff is not None:
            if aff.OrganizationObj.OrganizationTypeCV in ['Government agency', 'Research agency']:
                self.AgencyName = aff.OrganizationObj.OrganizationName
                self.AgencyCode = aff.OrganizationObj.OrganizationCode


class Series(wof_base.BaseSeries):
    """WOF Series Model."""
    def __init__(self, r=None, aff=None, tsrv=None):
        """Initialize WOF Series Object.

        :param r: ODM2 TimeSeriesResult Object
        :param aff: ODM2 Affiliation Object
        :param tsrv: ODM2 TimeSeriesResultValues Object
        """
        fa_obj = r.FeatureActionObj
        u_obj = r.UnitsObj
        v_obj = r.VariableObj
        p_obj = r.ProcessingLevelObj

        # sf_obj = fa_obj.SamplingFeatureObj
        a_obj = fa_obj.ActionObj
        m_obj = a_obj.MethodObj

        self.SeriesID = r.ResultID
        # self.SiteID = fa_obj.SamplingFeatureID
        # self.SiteCode = sf_obj.SamplingFeatureCode
        # self.SiteName = sf_obj.SamplingFeatureName
        self.Variable = Variable(v=v_obj, VarSampleMedium=r.SampledMediumCV,
                                 v_tunit=tsrv.TimeAggregationIntervalUnitsObj,
                                 v_timeinterval=tsrv.TimeAggregationInterval,
                                 v_unit=u_obj,
                                 aggregationstatisticCV=r.AggregationStatisticCV,
                                 actiontypeCV=a_obj.ActionTypeCV)
        # self.VariableID = r.VariableID
        # self.VariableCode = v_obj.VariableCode
        # self.VariableName = v_obj.VariableNameCV
        self.SampleMedium = r.SampledMediumCV
        self.QualityControlLevelID = p_obj.ProcessingLevelID
        self.QualityControlLevelCode = p_obj.ProcessingLevelCode
        self.Definition = p_obj.Definition
        self.Explanation = p_obj.Explanation
        # self.MethodID = m_obj.MethodID
        # self.MethodDescription = m_obj.MethodDescription
        self.Method = Method(m_obj)
        # TODO: Need Datetime handling, get offset from data
        self.BeginDateTimeUTC = a_obj.BeginDateTime.isoformat()
        if a_obj.EndDateTime is not None:
            self.EndDateTimeUTC = a_obj.EndDateTime.isoformat()
        else:
            self.EndDateTimeUTC = r.tsrv_EndDateTime.isoformat()

        self.ValueCount = r.ValueCount
        if aff is not None:
            if aff.OrganizationObj is not None:
                self.Organization = aff.OrganizationObj.OrganizationName
            self.Source = Source(aff)


def create_iso_utc_offset(utc_offset_hrs):
    """Create time offset from offset hours.

    :param utc_offset_hrs: UTC offset hours
    :return: UTC Offset iso string
    """
    hours = int(utc_offset_hrs)
    minutes = int((float(utc_offset_hrs) % 1) * 60)

    if hours == 0 and minutes == 0:
        return 'Z'
    else:
        return '%+.2d:%.2d' % (hours, minutes)


class DataValue(wof_base.BaseDataValue):
    """WOF DataValue Model."""
    def __init__(self, v, aff_obj=None):
        """Initialize WOF DataValue Object.

        :param v: ODM2 TimeSeriesResultValue Object
        :param aff_obj: ODM2 Affiliation Object
        """
        self.ValueID = v.ValueID
        self.DataValue = v.DataValue
        self.LocalDateTime = v.ValueDateTime.isoformat()
        localtime = datetime.strptime(self.LocalDateTime, '%Y-%m-%dT%H:%M:%S')
        self.DateTimeUTC = localtime+timedelta(hours=((-1)*int(v.ValueDateTimeUTCOffset)))
        self.UTCOffset = create_iso_utc_offset(v.ValueDateTimeUTCOffset)
        self.CensorCode = v.CensorCodeCV

        if aff_obj is not None:
            self.SourceID = '%d' % aff_obj.AffiliationID
            # self.SourceCode = aff_obj.OrganizationObj.OrganizationCode
        self.MethodID = v.ResultObj.FeatureActionObj.ActionObj.MethodObj.MethodID
        # self.MethodCode = v.ResultObj.FeatureActionObj.ActionObj.MethodObj.MethodCode
        self.QualityControlLevelID = v.ResultObj.ProcessingLevelObj.ProcessingLevelID
        # self.QualityControlLevel = QualityControlLevel(v.ResultObj.ProcessingLevelObj)
        self.QualityControlLevel = v.ResultObj.ProcessingLevelObj.ProcessingLevelCode


class Method(wof_base.BaseMethod):
    """WOF Method Model."""
    def __init__(self, m_obj):
        """Initialize WOF Method Object.

        :param m_obj: ODM2 Method Object
        """
        self.MethodID = m_obj.MethodID
        self.MethodDescription = m_obj.MethodDescription
        self.MethodLink = m_obj.MethodLink
        self.MethodCode = m_obj.MethodCode


class Unit(wof_base.BaseUnits):
    """WOF Unit Model."""
    def __init__(self, u_obj):
        """Initialize WOF Unit Object

        :param u_obj: ODM2 Unit Object
        """
        self.UnitsID = u_obj.UnitsID
        self.UnitsName = u_obj.UnitsName
        self.UnitsType = u_obj.UnitsTypeCV
        self.UnitsAbbreviation = u_obj.UnitsAbbreviation

        self.UnitsTypeValidate = False


class Source(wof_base.BaseSource):
    """WOF Source Model."""
    def __init__(self, aff_obj):
        """Initialize WOF Source Object

        :param aff_obj: ODM2 Affiliation Object
        """
        self.SourceID = aff_obj.AffiliationID

        if aff_obj.OrganizationObj is not None:
            self.Organization = aff_obj.OrganizationObj.OrganizationName
            self.SourceCode = self.SourceID
            self.SourceDescription = aff_obj.OrganizationObj.OrganizationDescription
            self.SourceLink = aff_obj.OrganizationObj.OrganizationLink

        self.ContactName = '%s %s' % (aff_obj.PersonObj.PersonFirstName,
                                      aff_obj.PersonObj.PersonLastName)

        if aff_obj.PrimaryPhone is not None:
            self.Phone = aff_obj.PrimaryPhone
        self.Email = aff_obj.PrimaryEmail
        if aff_obj.PrimaryAddress is not None:
            self.Address = aff_obj.PrimaryAddress
        # self.City = 'San Diego'
        # self.State = 'CA'
        # self.ZipCode = '92122'


class QualityControlLevel(wof_base.BaseQualityControlLevel):
    """WOF QualityControlLevel Model."""
    def __init__(self, p_obj):
        """Initialize WOF QualityControlLevel Object.

        :param p_obj: ODM2 ProcessingLevel Object
        """
        self.QualityControlLevelID = p_obj.ProcessingLevelID
        self.QualityControlLevelCode = p_obj.ProcessingLevelCode
        self.Definition = p_obj.Definition
        self.Explanation = p_obj.Explanation
